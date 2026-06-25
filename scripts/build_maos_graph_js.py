"""One-off builder: migrate maos web_ui graph JS into maos_observer/static/graph.js."""
from __future__ import annotations

from pathlib import Path

MAOS_UI = Path(__file__).resolve().parents[2] / "maos_job" / "web" / "web_ui.py"
OUT = Path(__file__).resolve().parents[1] / "src" / "sementic" / "maos_observer" / "static" / "graph.js"


def slice_between(text: str, start: str, end: str) -> str:
    return text[text.index(start) : text.index(end)]


def main() -> None:
    text = MAOS_UI.read_text(encoding="utf-8")
    graph_core = slice_between(text, "function drawGraph(state)", "function updateSidebar(task)")
    sidebar = slice_between(text, "function updateSidebar(task)", "function bindVisitHistoryActions")
    trace = slice_between(text, "function renderHermesPanels", "async function updateAgentInput")
    trace_step = slice_between(text, "function renderTraceStep(step)", "function renderTraceDuration")
    helpers = slice_between(text, "function statusLabel(status)", "function showError(message)")

    sidebar = sidebar.replace(
        "const task = tasks.find(item => item.task_id === activeTaskId) || {};",
        "const task = activeTask || {};",
    )
    graph_core = graph_core.replace("renderActiveTask();", "refreshGraphView();")
    graph_core = graph_core.replace("svg.setAttribute", "getSvg().setAttribute")
    graph_core = graph_core.replace("svg.innerHTML", "getSvg().innerHTML")
    graph_core = graph_core.replace("svg.appendChild", "getSvg().appendChild")

    header = """\
// Migrated from maos web/web_ui.py for sementic maos_observer (read-only, self-contained).
(function (global) {
  let selectedNodeId = null;
  let activeTask = null;

  function setActiveTask(task) {
    activeTask = task || null;
  }

  function getSelectedNodeId() {
    return selectedNodeId;
  }

  function getSvg() {
    return document.getElementById("graphSvg");
  }

  function refreshGraphView() {
    if (!activeTask || !activeTask.state) return;
    drawGraph(activeTask.state);
    updateSidebar(activeTask);
  }

"""
    footer = """
  function updateAgentTrace(node) {
    const traceEl = document.getElementById("agentTrace");
    const inputEl = document.getElementById("agentInput");
    const outputEl = document.getElementById("agentFinalOutput");
    const backend = String(node.backend || "simulator").toLowerCase();
    if (backend === "hermes") {
      renderHermesPanels(node, currentNodeResult(node));
      return;
    }
    if (backend === "multica_job") {
      renderMulticaJobPanels(node, currentNodeResult(node));
      return;
    }
    inputEl.innerHTML = `<div class="trace-empty">Node backend ${escapeHtml(backend)} — workflow state only.</div>`;
    outputEl.innerHTML = `<div class="trace-empty">No external Agent Service in maos_observer.</div>`;
    traceEl.innerHTML = `<div class="trace-empty">Inspect Workflow Result JSON for raw node output.</div>`;
  }

  function bindVisitHistoryActions(root) {
    root.querySelectorAll("[data-agent-task-id]").forEach((button) => {
      button.disabled = true;
      button.title = "Agent Service trace is not available in maos_observer";
    });
  }

  global.MaosGraph = {
    drawGraph,
    updateSidebar,
    setActiveTask,
    getSelectedNodeId,
    refreshGraphView,
    escapeHtml,
    safeStringify,
  };
})(window);
"""
    body = header + graph_core + sidebar + trace + trace_step + helpers + footer
    OUT.write_text(body, encoding="utf-8")
    print(f"wrote {OUT} ({len(body)} bytes)")


if __name__ == "__main__":
    main()
