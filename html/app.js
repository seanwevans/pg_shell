(function () {
  "use strict";

  const output = document.getElementById("output");

  function textElement(tagName, className, value) {
    const element = document.createElement(tagName);
    element.className = className;
    element.textContent = value == null ? "" : String(value);
    return element;
  }

  function renderPollResponse(value) {
    const rows = Array.isArray(value) ? value : [value];
    const fragment = document.createDocumentFragment();

    rows.forEach(function (row) {
      const result = document.createElement("article");
      result.className = "command-result";

      if (row !== null && typeof row === "object") {
        result.appendChild(textElement("pre", "command", row.command));
        result.appendChild(textElement("pre", "command-output", row.output));
        result.appendChild(textElement("span", "command-status", row.status));
        result.appendChild(textElement("span", "exit-code", row.exit_code));
      } else {
        result.appendChild(textElement("pre", "response-value", row));
      }
      fragment.appendChild(result);
    });

    output.replaceChildren(fragment);
  }

  function renderSubmissionResponse(value) {
    output.appendChild(textElement("div", "submission-response", value));
  }

  function parseResponse(xhr) {
    try {
      return JSON.parse(xhr.responseText);
    } catch (_error) {
      return xhr.responseText;
    }
  }

  document.body.addEventListener("htmx:afterRequest", function (event) {
    if (!event.detail.successful) {
      return;
    }

    const requestElement = event.detail.elt;
    const value = parseResponse(event.detail.xhr);
    if (requestElement === output) {
      renderPollResponse(value);
    } else if (requestElement.matches("form[hx-post='/rpc/submit_command']")) {
      renderSubmissionResponse(value);
    }
  });
}());
