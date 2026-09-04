// POSIX-style tokenizer matching the subset of Python's shlex.split(posix=True)
// that workers/executor_agent.py relies on: quote handling and backslash
// escapes, but no glob expansion, substitution, or operators. Error messages
// mirror shlex's so the demo surfaces the same failure text as the real
// executor, which stores str(exc) as the command output.

export class TokenizeError extends Error {}

const WHITESPACE = " \t\r\n";

export function split(input) {
  const tokens = [];
  let token = null;
  let index = 0;

  const push = (ch) => {
    token = token === null ? ch : token + ch;
  };

  while (index < input.length) {
    const ch = input[index++];

    if (WHITESPACE.includes(ch)) {
      if (token !== null) {
        tokens.push(token);
        token = null;
      }
      continue;
    }

    if (ch === "\\") {
      if (index >= input.length) throw new TokenizeError("No escaped character");
      push(input[index++]);
      continue;
    }

    if (ch === "'") {
      const end = input.indexOf("'", index);
      if (end === -1) throw new TokenizeError("No closing quotation");
      push(input.slice(index, end));
      if (end === index) token = token === null ? "" : token;
      index = end + 1;
      continue;
    }

    if (ch === '"') {
      let closed = false;
      if (token === null) token = "";
      while (index < input.length) {
        const inner = input[index++];
        if (inner === '"') {
          closed = true;
          break;
        }
        if (inner === "\\") {
          if (index >= input.length) throw new TokenizeError("No escaped character");
          const escaped = input[index++];
          // shlex only strips the backslash inside double quotes when it
          // escapes the quote character or another backslash; anything else
          // keeps the backslash literally.
          push('"\\'.includes(escaped) ? escaped : "\\" + escaped);
          continue;
        }
        push(inner);
      }
      if (!closed) throw new TokenizeError("No closing quotation");
      continue;
    }

    push(ch);
  }

  if (token !== null) tokens.push(token);
  return tokens;
}
