// The browser executor's command allowlist.
//
// executor_agent.py refuses anything outside EXECUTOR_ALLOWED_COMMANDS before
// it spawns a process. This table plays the same role here: it *is* the
// allowlist, and a name that is not in it is rejected with the executor's own
// error text rather than being interpreted in any way.

import { FsError, SHELL_ROOT } from "./vfs.js";

export class CommandError extends Error {
  constructor(message, exitCode = 1) {
    super(message);
    this.exitCode = exitCode;
  }
}

const MONTHS = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(" ");

function pad(value, width, filler = " ") {
  return String(value).padStart(width, filler);
}

function formatTime(mtime) {
  const at = new Date(mtime);
  const month = MONTHS[at.getUTCMonth()];
  const day = pad(at.getUTCDate(), 2);
  const time = `${pad(at.getUTCHours(), 2, "0")}:${pad(at.getUTCMinutes(), 2, "0")}`;
  return `${month} ${day} ${time}`;
}

function size(node) {
  return node.type === "dir" ? 4096 : new TextEncoder().encode(node.content).length;
}

// Split "-ln" style bundles into individual flags, stopping at "--" or the
// first operand, so options may be given in any order before the operands.
function parseFlags(argv, { valued = "" } = {}) {
  const flags = new Map();
  const operands = [];
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--") {
      operands.push(...argv.slice(index + 1));
      break;
    }
    if (argument.length > 1 && argument.startsWith("-") && !/^-\d+$/.test(argument)) {
      const letters = argument.slice(1).split("");
      for (let position = 0; position < letters.length; position += 1) {
        const letter = letters[position];
        if (valued.includes(letter)) {
          const inline = letters.slice(position + 1).join("");
          flags.set(letter, inline || argv[++index]);
          break;
        }
        flags.set(letter, true);
      }
      continue;
    }
    operands.push(argument);
  }
  return { flags, operands };
}

// Turn an FsError into the "prog: operand: reason" shape coreutils uses.
function fsFail(name, operand, error) {
  if (error instanceof FsError) throw new CommandError(`${name}: ${operand}: ${error.message}`);
  throw error;
}

function path(context, operand) {
  try {
    return context.fs.confine(operand, context.cwd);
  } catch (error) {
    return fsFail(context.name, operand, error);
  }
}

function readLines(text) {
  const lines = text.split("\n");
  if (lines.length && lines[lines.length - 1] === "") lines.pop();
  return lines;
}

function readOperandText(context, operand) {
  const target = path(context, operand);
  try {
    return context.fs.readFile(target);
  } catch (error) {
    return fsFail(context.name, operand, error);
  }
}

function countArgument(value, name) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new CommandError(`${name}: invalid number of lines: '${value}'`);
  }
  return parsed;
}

// Each entry receives { argv, cwd, env, fs, name } and returns stdout. Throwing
// a CommandError sets a non-zero exit code, which the executor records as a
// failed command.
export const COMMANDS = {
  help(context) {
    const names = Object.keys(COMMANDS).sort().join(" ");
    return [
      "This executor runs inside your browser tab and implements a fixed set of",
      "commands. Anything else is refused the way executor_agent.py refuses a",
      "binary that is not in EXECUTOR_ALLOWED_COMMANDS.",
      "",
      names,
      "",
      `Files live under ${SHELL_ROOT}; paths outside it are denied.`,
      "",
    ].join("\n");
  },

  pwd(context) {
    return context.cwd + "\n";
  },

  echo(context) {
    const { flags, operands } = parseFlags(context.argv);
    const text = operands.join(" ");
    return flags.get("n") ? text : text + "\n";
  },

  printf(context) {
    const [format, ...rest] = context.argv;
    if (format === undefined) throw new CommandError("printf: usage: printf format [arguments]");
    let index = 0;
    return format
      .replace(/\\n/g, "\n")
      .replace(/\\t/g, "\t")
      .replace(/%[sd%]/g, (match) => {
        if (match === "%%") return "%";
        const value = rest[index++] ?? "";
        return match === "%d" ? String(parseInt(value, 10) || 0) : value;
      });
  },

  ls(context) {
    const { flags, operands } = parseFlags(context.argv);
    const targets = operands.length ? operands : ["."];
    const blocks = [];
    for (const operand of targets) {
      const target = path(context, operand);
      let entries;
      try {
        entries = context.fs.list(target);
      } catch (error) {
        throw new CommandError(`ls: cannot access '${operand}': ${error.message}`, 2);
      }
      if (!flags.get("a")) entries = entries.filter((entry) => !entry.name.startsWith("."));
      const names = entries.map((entry) =>
        entry.node.type === "dir" && !flags.get("l") ? entry.name + "/" : entry.name
      );
      let body;
      if (flags.get("l")) {
        const widest = entries.reduce((width, entry) => Math.max(width, String(size(entry.node)).length), 1);
        body = entries
          .map((entry) => {
            const mode = entry.node.type === "dir" ? "drwxr-xr-x" : "-rw-r--r--";
            const links = entry.node.type === "dir" ? entry.node.children.size + 2 : 1;
            return [
              mode,
              pad(links, 2),
              "sandbox sandbox",
              pad(size(entry.node), widest),
              formatTime(entry.node.mtime),
              entry.name,
            ].join(" ");
          })
          .join("\n");
        // GNU ls reports the total in 1K blocks, not a file count.
        const blocks = entries.reduce((sum, entry) => sum + Math.ceil(size(entry.node) / 1024), 0);
        body = `total ${blocks}\n` + body;
      } else {
        body = flags.get("1") ? names.join("\n") : names.join("  ");
      }
      blocks.push(targets.length > 1 ? `${operand}:\n${body}` : body);
    }
    const text = blocks.join("\n\n");
    return text ? text + "\n" : "";
  },

  cat(context) {
    if (!context.argv.length) throw new CommandError("cat: no file operands");
    return context.argv.map((operand) => readOperandText(context, operand)).join("");
  },

  head(context) {
    const { flags, operands } = parseFlags(context.argv, { valued: "n" });
    const limit = flags.has("n") ? countArgument(flags.get("n"), "head") : 10;
    return operands
      .map((operand) => readLines(readOperandText(context, operand)).slice(0, limit).join("\n"))
      .filter((block) => block !== "")
      .map((block) => block + "\n")
      .join("");
  },

  tail(context) {
    const { flags, operands } = parseFlags(context.argv, { valued: "n" });
    const limit = flags.has("n") ? countArgument(flags.get("n"), "tail") : 10;
    return operands
      .map((operand) => {
        const lines = readLines(readOperandText(context, operand));
        return lines.slice(Math.max(0, lines.length - limit)).join("\n");
      })
      .filter((block) => block !== "")
      .map((block) => block + "\n")
      .join("");
  },

  wc(context) {
    const { flags, operands } = parseFlags(context.argv);
    const selected = ["l", "w", "c"].filter((flag) => flags.get(flag));
    const columns = selected.length ? selected : ["l", "w", "c"];
    const totals = { l: 0, w: 0, c: 0 };
    const rows = operands.map((operand) => {
      const text = readOperandText(context, operand);
      const counts = {
        l: readLines(text).length,
        w: text.split(/\s+/).filter(Boolean).length,
        c: new TextEncoder().encode(text).length,
      };
      for (const column of columns) totals[column] += counts[column];
      return columns.map((column) => pad(counts[column], 7)).join(" ") + " " + operand;
    });
    if (rows.length > 1) {
      rows.push(columns.map((column) => pad(totals[column], 7)).join(" ") + " total");
    }
    return rows.length ? rows.join("\n") + "\n" : "";
  },

  grep(context) {
    const { flags, operands } = parseFlags(context.argv);
    const [pattern, ...files] = operands;
    if (pattern === undefined || !files.length) {
      throw new CommandError("grep: usage: grep [-inv] PATTERN FILE...", 2);
    }
    let matcher;
    try {
      matcher = new RegExp(pattern, flags.get("i") ? "i" : "");
    } catch (error) {
      throw new CommandError(`grep: ${error.message}`, 2);
    }
    const invert = Boolean(flags.get("v"));
    const showNames = files.length > 1;
    const out = [];
    for (const operand of files) {
      readLines(readOperandText(context, operand)).forEach((line, index) => {
        if (matcher.test(line) === invert) return;
        const prefix = [showNames ? operand : null, flags.get("n") ? index + 1 : null]
          .filter((part) => part !== null)
          .join(":");
        out.push(prefix ? `${prefix}:${line}` : line);
      });
    }
    // grep exits 1 when nothing matched, which surfaces as a failed command.
    if (!out.length) throw new CommandError("", 1);
    return out.join("\n") + "\n";
  },

  find(context) {
    const { flags, operands } = parseFlags(context.argv, { valued: "" });
    const nameIndex = context.argv.indexOf("-name");
    const pattern = nameIndex === -1 ? null : context.argv[nameIndex + 1];
    const roots = operands.filter((operand) => operand !== "-name" && operand !== pattern);
    const start = roots.length ? roots : ["."];
    const matches = [];
    const glob = pattern
      ? new RegExp("^" + pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*").replace(/\?/g, ".") + "$")
      : null;

    const walk = (absolute, display) => {
      const base = display.split("/").pop() || display;
      if (!glob || glob.test(base)) matches.push(display);
      if (!context.fs.isDir(absolute)) return;
      for (const entry of context.fs.list(absolute)) {
        walk(absolute + "/" + entry.name, display === "/" ? "/" + entry.name : display + "/" + entry.name);
      }
    };

    for (const operand of start) {
      const target = path(context, operand);
      if (!context.fs.exists(target)) {
        throw new CommandError(`find: '${operand}': No such file or directory`);
      }
      walk(target, operand);
    }
    return matches.length ? matches.join("\n") + "\n" : "";
  },

  mkdir(context) {
    const { flags, operands } = parseFlags(context.argv);
    if (!operands.length) throw new CommandError("mkdir: missing operand");
    for (const operand of operands) {
      const target = path(context, operand);
      try {
        context.fs.mkdir(target, { parents: Boolean(flags.get("p")) });
      } catch (error) {
        if (flags.get("p") && error instanceof FsError && error.message === "File exists") continue;
        fsFail("mkdir", operand, error);
      }
    }
    return "";
  },

  rmdir(context) {
    if (!context.argv.length) throw new CommandError("rmdir: missing operand");
    for (const operand of context.argv) {
      const target = path(context, operand);
      if (!context.fs.isDir(target)) {
        throw new CommandError(`rmdir: failed to remove '${operand}': Not a directory`);
      }
      try {
        context.fs.remove(target);
      } catch (error) {
        fsFail("rmdir", `failed to remove '${operand}'`, error);
      }
    }
    return "";
  },

  touch(context) {
    if (!context.argv.length) throw new CommandError("touch: missing file operand");
    for (const operand of context.argv) {
      const target = path(context, operand);
      try {
        context.fs.touch(target);
      } catch (error) {
        fsFail("touch", operand, error);
      }
    }
    return "";
  },

  rm(context) {
    const { flags, operands } = parseFlags(context.argv);
    const recursive = Boolean(flags.get("r") || flags.get("R"));
    const force = Boolean(flags.get("f"));
    if (!operands.length && !force) throw new CommandError("rm: missing operand");
    for (const operand of operands) {
      const target = path(context, operand);
      try {
        context.fs.remove(target, { recursive });
      } catch (error) {
        if (force && error instanceof FsError && error.message === "No such file or directory") continue;
        fsFail("rm", `cannot remove '${operand}'`, error);
      }
    }
    return "";
  },

  cp(context) {
    const { flags, operands } = parseFlags(context.argv);
    if (operands.length < 2) throw new CommandError("cp: missing destination file operand");
    const destination = operands.pop();
    const target = path(context, destination);
    if (operands.length > 1 && !context.fs.isDir(target)) {
      throw new CommandError(`cp: target '${destination}' is not a directory`);
    }
    for (const operand of operands) {
      try {
        context.fs.copy(path(context, operand), target, {
          recursive: Boolean(flags.get("r") || flags.get("R")),
        });
      } catch (error) {
        fsFail("cp", `cannot copy '${operand}'`, error);
      }
    }
    return "";
  },

  mv(context) {
    const { operands } = parseFlags(context.argv);
    if (operands.length < 2) throw new CommandError("mv: missing destination file operand");
    const destination = operands.pop();
    const target = path(context, destination);
    if (operands.length > 1 && !context.fs.isDir(target)) {
      throw new CommandError(`mv: target '${destination}' is not a directory`);
    }
    for (const operand of operands) {
      try {
        context.fs.move(path(context, operand), target);
      } catch (error) {
        fsFail("mv", `cannot move '${operand}'`, error);
      }
    }
    return "";
  },

  sort(context) {
    const { flags, operands } = parseFlags(context.argv);
    const lines = operands.flatMap((operand) => readLines(readOperandText(context, operand)));
    lines.sort((a, b) =>
      flags.get("n") ? Number(a) - Number(b) || (a < b ? -1 : a > b ? 1 : 0) : a < b ? -1 : a > b ? 1 : 0
    );
    if (flags.get("r")) lines.reverse();
    return lines.length ? lines.join("\n") + "\n" : "";
  },

  uniq(context) {
    const { flags, operands } = parseFlags(context.argv);
    const lines = operands.flatMap((operand) => readLines(readOperandText(context, operand)));
    const out = [];
    for (const line of lines) {
      const previous = out[out.length - 1];
      if (previous && previous.line === line) previous.count += 1;
      else out.push({ line, count: 1 });
    }
    const rendered = out.map((entry) => (flags.get("c") ? `${pad(entry.count, 7)} ${entry.line}` : entry.line));
    return rendered.length ? rendered.join("\n") + "\n" : "";
  },

  seq(context) {
    const numbers = context.argv.map(Number);
    if (!numbers.length || numbers.some((value) => !Number.isFinite(value))) {
      throw new CommandError("seq: usage: seq [FIRST [STEP]] LAST");
    }
    const [first, step, last] =
      numbers.length === 1
        ? [1, 1, numbers[0]]
        : numbers.length === 2
          ? [numbers[0], 1, numbers[1]]
          : numbers;
    if (step === 0) throw new CommandError("seq: step must not be zero");
    const out = [];
    for (let value = first; step > 0 ? value <= last : value >= last; value += step) out.push(value);
    return out.length ? out.join("\n") + "\n" : "";
  },

  basename(context) {
    const [operand] = context.argv;
    if (operand === undefined) throw new CommandError("basename: missing operand");
    return (operand.replace(/\/+$/, "").split("/").pop() || "/") + "\n";
  },

  dirname(context) {
    const [operand] = context.argv;
    if (operand === undefined) throw new CommandError("dirname: missing operand");
    const trimmed = operand.replace(/\/+$/, "");
    const cut = trimmed.lastIndexOf("/");
    return (cut === -1 ? "." : cut === 0 ? "/" : trimmed.slice(0, cut)) + "\n";
  },

  env(context) {
    return Object.entries(context.env)
      .map(([key, value]) => `${key}=${value}`)
      .sort()
      .join("\n")
      .concat("\n");
  },

  date() {
    return new Date().toUTCString() + "\n";
  },

  whoami() {
    return "sandbox\n";
  },

  id() {
    return "uid=1000(sandbox) gid=1000(sandbox) groups=1000(sandbox)\n";
  },

  hostname() {
    return "pg-shell\n";
  },

  uname(context) {
    const { flags } = parseFlags(context.argv);
    return (flags.get("a") ? "Linux pg-shell 0.0.0-wasm #1 SMP wasm32 GNU/Linux" : "Linux") + "\n";
  },

  true() {
    return "";
  },

  false() {
    throw new CommandError("", 1);
  },

  async sleep(context) {
    const seconds = Number(context.argv[0]);
    if (!Number.isFinite(seconds) || seconds < 0) {
      throw new CommandError(`sleep: invalid time interval '${context.argv[0] ?? ""}'`);
    }
    await new Promise((resolve) => setTimeout(resolve, seconds * 1000));
    return "";
  },
};

export const ALLOWED_COMMANDS = Object.keys(COMMANDS).sort();
