(function () {
  const statusFilter = document.getElementById("statusFilter");
  const refreshBtn = document.getElementById("refreshBtn");
  const exportLink = document.getElementById("exportLink");
  const taskBoardBody = document.getElementById("taskBoardBody");
  const runtimeMeta = document.getElementById("runtimeMeta");
  const errorBox = document.getElementById("errorBox");
  const updatedAt = document.getElementById("updatedAt");

  let tasks = [];
  let activeTaskId = "";
  let refreshTimer = null;

  function showError(message) {
    errorBox.style.display = "block";
    errorBox.textContent = message;
  }

  function clearError() {
    errorBox.style.display = "none";
    errorBox.textContent = "";
  }

  function nodeProgress(task) {
    const nodes = (task.nodes || (task.state && task.state.nodes) || []);
    const done = nodes.filter((node) => node.status === "completed").length;
    return `${done}/${nodes.length}`;
  }

  function renderRows(items) {
    if (!items.length) {
      taskBoardBody.innerHTML = '<tr><td colspan="5" class="muted">暂无任务</td></tr>';
      return;
    }
    taskBoardBody.innerHTML = items
      .map((task) => {
        const active = task.task_id === activeTaskId ? "active-row" : "";
        return `
          <tr class="${active}" data-task-id="${MaosGraph.escapeHtml(task.task_id)}">
            <td>
              <strong>${MaosGraph.escapeHtml(task.graph_name || task.graph_id || task.task_id)}</strong>
              <div class="muted">${MaosGraph.escapeHtml(task.task_id || "")}</div>
            </td>
            <td>
              <span class="status-chip">
                <span class="status-dot dot-${MaosGraph.escapeHtml(task.status)}"></span>
                ${MaosGraph.escapeHtml(task.status)}
              </span>
            </td>
            <td>${MaosGraph.escapeHtml(nodeProgress(task))}</td>
            <td>${MaosGraph.escapeHtml(task.event_id || "-")}</td>
            <td>${MaosGraph.escapeHtml(task.submitted_at || "-")}</td>
          </tr>`;
      })
      .join("");

    taskBoardBody.querySelectorAll("tr[data-task-id]").forEach((row) => {
      row.addEventListener("click", () => {
        activeTaskId = row.getAttribute("data-task-id") || "";
        renderRows(tasks);
        loadTaskDetail(activeTaskId);
      });
    });
  }

  async function loadTasks() {
    clearError();
    const status = statusFilter.value;
    exportLink.href = `/api/tasks/export?status=${encodeURIComponent(status)}`;
    const response = await fetch(`/api/tasks?status=${encodeURIComponent(status)}`, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`加载任务失败: HTTP ${response.status}`);
    }
    const payload = await response.json();
    tasks = payload.tasks || [];
    runtimeMeta.textContent = `runtime=${payload.runtime_status || "-"} · ${payload.task_count || 0} 条 · filter=${payload.filter}`;
    updatedAt.textContent = new Date().toLocaleTimeString();
    if (!activeTaskId && tasks.length) {
      activeTaskId = tasks[0].task_id;
    }
    renderRows(tasks);
    if (activeTaskId) {
      await loadTaskDetail(activeTaskId);
    } else {
      MaosGraph.setActiveTask(null);
      MaosGraph.drawGraph({ nodes: [], edges: [] });
      document.getElementById("resultBox").textContent = "暂无任务";
    }
  }

  async function loadTaskDetail(taskId) {
    if (!taskId) return;
    const response = await fetch(`/api/tasks/${encodeURIComponent(taskId)}`, { cache: "no-store" });
    if (!response.ok) {
      showError(`加载任务详情失败: HTTP ${response.status}`);
      return;
    }
    const detail = await response.json();
    const task = {
      task_id: detail.task_id,
      workflow_id: detail.workflow_id,
      graph_id: detail.graph_id,
      graph_name: detail.graph_name,
      status: detail.status,
      state: detail.state || {},
      result: detail.result,
      error: detail.error,
    };
    MaosGraph.setActiveTask(task);
    MaosGraph.drawGraph(task.state || { nodes: [], edges: [] });
    MaosGraph.updateSidebar(task);
  }

  async function refreshAll() {
    try {
      await loadTasks();
    } catch (error) {
      showError(error.message || String(error));
    }
  }

  refreshBtn.addEventListener("click", refreshAll);
  statusFilter.addEventListener("change", () => {
    activeTaskId = "";
    refreshAll();
  });

  refreshAll();
  refreshTimer = window.setInterval(refreshAll, 5000);
  window.addEventListener("beforeunload", () => window.clearInterval(refreshTimer));
})();
