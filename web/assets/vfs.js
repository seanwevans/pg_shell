// In-memory filesystem for the browser executor.
//
// The real executor shells out to the host and relies on OS permissions plus
// EXECUTOR_ALLOWED_COMMANDS for isolation. Nothing like that exists in a tab,
// so this filesystem is the isolation boundary instead: every path resolves
// under SHELL_ROOT and anything outside it is refused, mirroring the
// confinement a correctly configured deployment gets from the host.

export const SHELL_ROOT = "/home/sandbox";

export class FsError extends Error {}

// Seed entries share a fixed timestamp so `ls -l` renders identically on every
// load; anything the session creates is stamped with the current time.
const SEED_MTIME = Date.UTC(2024, 4, 1, 9, 14, 0);

function dir(children = {}, mtime = SEED_MTIME) {
  return { type: "dir", children: new Map(Object.entries(children)), mtime };
}

function file(content = "", mtime = SEED_MTIME) {
  return { type: "file", content, mtime };
}

function now() {
  return Date.now();
}

const SEED = () =>
  dir({
    "README.md": file(
      [
        "pg_shell — everything you type here is a row in PostgreSQL.",
        "",
        "The page you are looking at runs a real PostgreSQL database compiled",
        "to WebAssembly. Commands are inserted into the `commands` table by the",
        "submit_command() function, picked up by an executor, and written back",
        "as output. Nothing is kept in browser state: reload the page and the",
        "transcript is rebuilt from the database.",
        "",
        "Try:  ls -l    cat notes/todo.txt    grep -n shell logs/audit.log",
        "Then open the Database panel and run: SELECT * FROM commands;",
        "",
      ].join("\n")
    ),
    "hello.txt": file("hello from inside the database\n"),
    notes: dir({
      "todo.txt": file(
        [
          "- read SPEC.md",
          "- replay a session from command 1",
          "- fork a session off a command snapshot",
          "- check that env_snapshot is carried per command",
          "",
        ].join("\n")
      ),
      "spec-notes.md": file(
        [
          "# Notes on SPEC.md",
          "",
          "Sessions are explicit and owned by a user. A command carries the",
          "cwd and env it was claimed with, which is what makes replay",
          "deterministic and fork possible.",
          "",
        ].join("\n")
      ),
    }),
    logs: dir({
      "audit.log": file(
        [
          "2024-05-01T09:14:02Z session opened",
          "2024-05-01T09:14:19Z shell command accepted: pwd",
          "2024-05-01T09:15:47Z shell command accepted: ls -l",
          "2024-05-01T09:16:03Z shell command rejected: not on allowlist",
          "2024-05-01T09:18:41Z session idle",
          "",
        ].join("\n")
      ),
      "executor.log": file(
        [
          "worker=browser lease=60s poll=notify",
          "claimed 1 command(s)",
          "",
        ].join("\n")
      ),
    }),
  });

export class Vfs {
  constructor() {
    this.reset();
  }

  reset() {
    this.tree = dir({ home: dir({ sandbox: SEED() }) });
  }

  // --- path handling ---------------------------------------------------

  // Pure lexical normalisation, matching os.path.realpath on a tree without
  // symlinks: this is what executor_agent.py resolves `cd` targets with.
  resolve(path, cwd = SHELL_ROOT) {
    const base = path.startsWith("/") ? "/" : cwd;
    const parts = (base + "/" + path).split("/");
    const stack = [];
    for (const part of parts) {
      if (part === "" || part === ".") continue;
      if (part === "..") stack.pop();
      else stack.push(part);
    }
    return "/" + stack.join("/");
  }

  inRoot(absolute) {
    return absolute === SHELL_ROOT || absolute.startsWith(SHELL_ROOT + "/");
  }

  // Resolve and confine in one step. Callers get an absolute path they are
  // allowed to touch, or an FsError they can report as the command's output.
  confine(path, cwd) {
    const absolute = this.resolve(path, cwd);
    if (!this.inRoot(absolute)) throw new FsError("Permission denied");
    return absolute;
  }

  // --- lookups ---------------------------------------------------------

  lookup(absolute) {
    if (absolute === "/") return this.tree;
    let node = this.tree;
    for (const part of absolute.split("/").slice(1)) {
      if (node.type !== "dir") return null;
      node = node.children.get(part);
      if (!node) return null;
    }
    return node;
  }

  parentOf(absolute) {
    const cut = absolute.lastIndexOf("/");
    return [cut === 0 ? "/" : absolute.slice(0, cut), absolute.slice(cut + 1)];
  }

  exists(absolute) {
    return this.lookup(absolute) !== null;
  }

  isDir(absolute) {
    const node = this.lookup(absolute);
    return node !== null && node.type === "dir";
  }

  readFile(absolute) {
    const node = this.lookup(absolute);
    if (node === null) throw new FsError("No such file or directory");
    if (node.type === "dir") throw new FsError("Is a directory");
    return node.content;
  }

  list(absolute) {
    const node = this.lookup(absolute);
    if (node === null) throw new FsError("No such file or directory");
    if (node.type === "file") return [{ name: this.parentOf(absolute)[1], node }];
    return [...node.children.entries()]
      .map(([name, child]) => ({ name, node: child }))
      .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  }

  // --- mutations -------------------------------------------------------

  requireParentDir(absolute) {
    const [parentPath, name] = this.parentOf(absolute);
    const parent = this.lookup(parentPath);
    if (parent === null) throw new FsError("No such file or directory");
    if (parent.type !== "dir") throw new FsError("Not a directory");
    return [parent, name];
  }

  writeFile(absolute, content) {
    const [parent, name] = this.requireParentDir(absolute);
    const existing = parent.children.get(name);
    if (existing && existing.type === "dir") throw new FsError("Is a directory");
    parent.children.set(name, file(content, now()));
  }

  touch(absolute) {
    const [parent, name] = this.requireParentDir(absolute);
    const existing = parent.children.get(name);
    if (existing) existing.mtime = now();
    else parent.children.set(name, file("", now()));
  }

  mkdir(absolute, { parents = false } = {}) {
    if (parents) {
      let walked = "";
      for (const part of absolute.split("/").slice(1)) {
        walked += "/" + part;
        const node = this.lookup(walked);
        if (node === null) {
          const [parent, name] = this.requireParentDir(walked);
          parent.children.set(name, dir({}, now()));
        } else if (node.type !== "dir") {
          throw new FsError("Not a directory");
        }
      }
      return;
    }
    if (this.exists(absolute)) throw new FsError("File exists");
    const [parent, name] = this.requireParentDir(absolute);
    parent.children.set(name, dir({}, now()));
  }

  remove(absolute, { recursive = false } = {}) {
    if (absolute === SHELL_ROOT) throw new FsError("Permission denied");
    const node = this.lookup(absolute);
    if (node === null) throw new FsError("No such file or directory");
    if (node.type === "dir" && !recursive && node.children.size > 0) {
      throw new FsError("Directory not empty");
    }
    if (node.type === "dir" && !recursive) throw new FsError("Is a directory");
    const [parent, name] = this.requireParentDir(absolute);
    parent.children.delete(name);
  }

  clone(node) {
    if (node.type === "file") return file(node.content, now());
    const copy = dir({}, now());
    for (const [name, child] of node.children) copy.children.set(name, this.clone(child));
    return copy;
  }

  copy(fromAbsolute, toAbsolute, { recursive = false } = {}) {
    const source = this.lookup(fromAbsolute);
    if (source === null) throw new FsError("No such file or directory");
    if (source.type === "dir" && !recursive) throw new FsError("Is a directory");
    const destination = this.isDir(toAbsolute)
      ? toAbsolute + "/" + this.parentOf(fromAbsolute)[1]
      : toAbsolute;
    const [parent, name] = this.requireParentDir(destination);
    parent.children.set(name, this.clone(source));
  }

  move(fromAbsolute, toAbsolute) {
    if (fromAbsolute === SHELL_ROOT) throw new FsError("Permission denied");
    const source = this.lookup(fromAbsolute);
    if (source === null) throw new FsError("No such file or directory");
    const destination = this.isDir(toAbsolute)
      ? toAbsolute + "/" + this.parentOf(fromAbsolute)[1]
      : toAbsolute;
    if (destination === fromAbsolute) return;
    if (destination.startsWith(fromAbsolute + "/")) {
      throw new FsError("cannot move a directory into itself");
    }
    const [parent, name] = this.requireParentDir(destination);
    parent.children.set(name, source);
    const [oldParent, oldName] = this.requireParentDir(fromAbsolute);
    oldParent.children.delete(oldName);
  }
}
