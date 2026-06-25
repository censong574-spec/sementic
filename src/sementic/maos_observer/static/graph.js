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

function drawGraph(state) {
      const nodes = state.nodes || [];
      const layout = computeLayout(state);
      const visualInfo = graphVisualInfo(state, layout);
      const points = Object.values(layout);
      const width = Math.max(980, ...points.map(point => point.x + 190));
      const height = Math.max(720, ...points.map(point => point.y + 112));
      getSvg().setAttribute("viewBox", `0 0 ${width} ${height}`);
      getSvg().innerHTML = `
        <defs>
          <marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
            <path d="M0,0 L0,6 L9,3 z" fill="#8b97a7"></path>
          </marker>
          <marker id="arrow-conditional" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
            <path d="M0,0 L0,6 L9,3 z" fill="#c77700"></path>
          </marker>
          <marker id="arrow-loop" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
            <path d="M0,0 L0,6 L9,3 z" fill="#7d4bc7"></path>
          </marker>
        </defs>
      `;
      if (!nodes.length) {
        getSvg().appendChild(svgEl("text", { x: 40, y: 70, class: "node-meta" }, "Batch-load JSON control-flow graphs and click Run Loaded Batch."));
        return;
      }
      const edgeLabels = [];
      for (const edge of state.edges || []) {
        const from = layout[edge.from];
        const to = layout[edge.to];
        if (!from || !to) continue;
        const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        const edgeKind = classifyEdge(edge, from, to);
        const geom = edgeGeometry(edgeKind, from, to);
        path.setAttribute("class", edgeClass(edge, edgeKind));
        path.setAttribute("d", geom.d);
        getSvg().appendChild(path);
        const label = edge.label || edge.when || edge.kind || "";
        if (label || edge.taken_count || edge.skipped_count) {
          const labelText = [
            label ? truncate(String(label), 34) : "",
            edge.taken_count ? `taken ${edge.taken_count}` : "",
            edge.skipped_count ? `skip ${edge.skipped_count}` : "",
          ].filter(Boolean).join(" | ");
          edgeLabels.push({
            x: geom.labelX,
            y: geom.labelY,
            anchor: geom.labelAnchor || "start",
            kind: edgeKind,
            text: labelText,
          });
        }
      }
      for (const node of nodes) {
        const point = layout[node.id];
        if (point) getSvg().appendChild(createNode(node, point.x, point.y, visualInfo));
      }
      for (const label of edgeLabels) {
        getSvg().appendChild(createEdgeLabel(label));
      }
    }

    function createEdgeLabel(label) {
      const group = svgEl("g", { class: "edge-label-group" });
      const width = Math.min(260, Math.max(54, label.text.length * 6.4 + 14));
      const x = Math.max(8, label.x);
      const y = Math.max(18, label.y);
      const anchor = label.anchor || "start";
      const textX = x;
      const rectX = anchor === "end" ? x - width - 7 : x - 7;
      group.appendChild(svgEl("rect", {
        x: rectX,
        y: y - 14,
        width,
        height: 19,
        rx: 5,
        class: "edge-label-bg",
      }));
      group.appendChild(svgEl("text", {
        x: textX,
        y,
        "text-anchor": anchor,
        class: `edge-label ${label.kind}`,
      }, label.text));
      return group;
    }

    function graphVisualInfo(state, layout) {
      const branchNodeIds = new Set();
      const loopNodeIds = new Set();
      for (const edge of state.edges || []) {
        const from = layout[edge.from];
        const to = layout[edge.to];
        if (!from || !to) continue;
        if (edge.when) branchNodeIds.add(edge.from);
        if (isLoopEdgeKind(classifyEdge(edge, from, to))) {
          loopNodeIds.add(edge.from);
          loopNodeIds.add(edge.to);
        }
      }
      return { branchNodeIds, loopNodeIds };
    }

    function classifyEdge(edge, from, to) {
      if (to.y <= from.y) return edge.when ? "conditional-loop" : "loop";
      if (edge.when) return "conditional";
      return "normal";
    }

    function isLoopEdgeKind(edgeKind) {
      return edgeKind === "loop" || edgeKind === "conditional-loop";
    }

    function edgeClass(edge, edgeKind) {
      const classes = ["edge"];
      if (edgeKind !== "normal") classes.push(edgeKind);
      if (Number(edge.taken_count || 0) > 0) classes.push("taken");
      if (Number(edge.skipped_count || 0) > 0 && Number(edge.taken_count || 0) === 0) classes.push("skipped");
      return classes.join(" ");
    }

    function edgeGeometry(edgeKind, from, to) {
      if (edgeKind === "conditional-loop") {
        const useLeftRail = to.x <= from.x;
        const side = useLeftRail ? -1 : 1;
        const startX = useLeftRail ? from.x + 10 : from.x + 170;
        const startY = from.y + 32;
        const endX = useLeftRail ? to.x + 10 : to.x + 170;
        const endY = to.y + 54;
        const railGap = 104;
        const railX = useLeftRail
          ? Math.min(startX, endX) - railGap
          : Math.max(startX, endX) + railGap;
        const radius = 18;
        const verticalSign = endY < startY ? -1 : 1;
        const railCornerX = railX - side * radius;
        const firstVerticalY = startY + verticalSign * radius;
        const secondVerticalY = endY - verticalSign * radius;
        const labelAnchor = useLeftRail ? "end" : "start";
        const labelY = (startY + endY) / 2 - 8;
        return {
          d: [
            `M ${startX} ${startY}`,
            `L ${railCornerX} ${startY}`,
            `Q ${railX} ${startY} ${railX} ${firstVerticalY}`,
            `L ${railX} ${secondVerticalY}`,
            `Q ${railX} ${endY} ${railCornerX} ${endY}`,
            `L ${endX} ${endY}`,
          ].join(" "),
          labelX: railX + (useLeftRail ? -12 : 12),
          labelY,
          labelAnchor,
        };
      }
      if (edgeKind === "loop") {
        const startX = from.x + 16;
        const startY = from.y + 38;
        const endX = to.x + 164;
        const endY = to.y + 38;
        const sideX = Math.min(startX, endX) - 74;
        const liftY = Math.min(startY, endY) - 50;
        return {
          d: `M ${startX} ${startY} C ${sideX} ${startY}, ${sideX} ${liftY}, ${sideX + 34} ${liftY} S ${endX} ${liftY}, ${endX} ${endY}`,
          labelX: sideX + 12,
          labelY: liftY - 8,
        };
      }
      const startX = from.x + 90;
      const startY = from.y + 78;
      const endX = to.x + 90;
      const endY = to.y;
      const midY = (startY + endY) / 2;
      const horizontalDelta = endX - startX;
      const isConditional = edgeKind === "conditional";
      const labelAnchor = isConditional && horizontalDelta < 0 ? "end" : "start";
      const labelOffset = isConditional
        ? (horizontalDelta < 0 ? -44 : 44)
        : 8;
      return {
        d: `M ${startX} ${startY} C ${startX} ${midY}, ${endX} ${midY}, ${endX} ${endY}`,
        labelX: (startX + endX) / 2 + labelOffset,
        labelY: midY - 8,
        labelAnchor,
      };
    }

    function computeLayout(state) {
      const levels = buildVisualLevels(state);
      const layout = {};
      const widest = levels.length ? Math.max(...levels.map(level => level.length)) : 1;
      const width = Math.max(980, widest * 280 + 160);
      const levelGap = 132;
      const nodeGap = 280;
      levels.forEach((level, levelIndex) => {
        const rowWidth = (level.length - 1) * nodeGap;
        const startX = (width - rowWidth) / 2 - 90;
        level.forEach((nodeId, index) => {
          layout[nodeId] = { x: startX + index * nodeGap, y: 42 + levelIndex * levelGap };
        });
      });
      return layout;
    }

    function buildVisualLevels(state) {
      const nodes = state.nodes || [];
      const edges = state.edges || [];
      const nodeIds = nodes.map(node => node.id);
      if (!nodeIds.length) return [];
      const nodeSet = new Set(nodeIds);
      const nodeOrder = {};
      nodeIds.forEach((nodeId, index) => {
        nodeOrder[nodeId] = index;
      });
      const outgoing = {};
      for (const edge of edges) {
        if (!nodeSet.has(edge.from) || !nodeSet.has(edge.to)) continue;
        if (!outgoing[edge.from]) outgoing[edge.from] = [];
        outgoing[edge.from].push(edge);
      }

      function reaches(start, target, skipEdge) {
        if (start === target) return true;
        const seen = new Set();
        const stack = [start];
        while (stack.length) {
          const current = stack.pop();
          if (current === target) return true;
          if (seen.has(current)) continue;
          seen.add(current);
          for (const edge of outgoing[current] || []) {
            if (edge === skipEdge) continue;
            if (nodeSet.has(edge.to) && !seen.has(edge.to)) stack.push(edge.to);
          }
        }
        return false;
      }

      const loopEdgeSet = new Set();
      for (const edge of edges) {
        if (!nodeSet.has(edge.from) || !nodeSet.has(edge.to)) continue;
        const pointsBack = (nodeOrder[edge.to] ?? 0) <= (nodeOrder[edge.from] ?? 0);
        if (edge.from === edge.to || (pointsBack && reaches(edge.to, edge.from, edge))) {
          loopEdgeSet.add(edge);
        }
      }
      const forwardEdges = edges.filter(edge =>
        nodeSet.has(edge.from) && nodeSet.has(edge.to) && !loopEdgeSet.has(edge)
      );

      const levelById = {};
      const orderById = {};
      nodeIds.forEach((nodeId, index) => {
        levelById[nodeId] = 0;
        orderById[nodeId] = index * 100;
      });

      let changed = true;
      let guard = 0;
      while (changed && guard < 100) {
        changed = false;
        guard += 1;
        for (const edge of forwardEdges) {
          const requiredLevel = (levelById[edge.from] ?? 0) + 1;
          if ((levelById[edge.to] ?? 0) < requiredLevel) {
            levelById[edge.to] = requiredLevel;
            changed = true;
          }
        }
      }

      for (const [from, fromEdges] of Object.entries(outgoing)) {
        const visualFanout = fromEdges.filter(edge => !loopEdgeSet.has(edge));
        if (visualFanout.length < 2) continue;
        const branchLevel = (levelById[from] ?? 0) + 1;
        const center = (visualFanout.length - 1) / 2;
        visualFanout.forEach((edge, index) => {
          if ((levelById[edge.to] ?? 0) === branchLevel) {
            orderById[edge.to] = (orderById[from] ?? 0) + (index - center) * 10;
          }
        });
      }

      const grouped = {};
      for (const nodeId of nodeIds) {
        const level = levelById[nodeId] ?? 0;
        if (!grouped[level]) grouped[level] = [];
        grouped[level].push(nodeId);
      }
      return Object.keys(grouped)
        .map(Number)
        .sort((a, b) => a - b)
        .map(level => grouped[level].sort((a, b) => (orderById[a] ?? 0) - (orderById[b] ?? 0)));
    }

    function createNode(node, x, y, visualInfo) {
      const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
      const nodeType = nodeKind(node, visualInfo);
      const loopable = Number(node.max_visits || 1) > 1 || visualInfo.loopNodeIds.has(node.id);
      g.setAttribute("class", `node ${node.status} type-${nodeType}${loopable ? " loopable" : ""}${selectedNodeId === node.id ? " selected" : ""}`);
      g.setAttribute("transform", `translate(${x}, ${y})`);
      g.style.cursor = "pointer";
      g.addEventListener("click", () => {
        selectedNodeId = node.id;
        refreshGraphView();
      });
      const shape = nodeType === "condition"
        ? svgEl("polygon", { points: "90,0 180,39 90,78 0,39", class: "node-shape" })
        : svgEl("rect", { width: 180, height: 78, class: "node-shape" });
      const ring = loopable
        ? (nodeType === "condition"
          ? svgEl("polygon", { points: "90,-7 190,39 90,85 -10,39", class: "loop-ring" })
          : svgEl("rect", { x: -6, y: -6, width: 192, height: 90, rx: 10, class: "loop-ring" }))
        : null;
      const badgeBg = svgEl("rect", { x: 12, y: 12, width: 74, height: 22, rx: 6, class: `badge-bg ${node.status}` });
      const badge = svgEl("text", { x: 49, y: 27, "text-anchor": "middle", class: "badge" }, statusLabel(node.status));
      const kindText = nodeType === "condition" ? "IF" : (nodeType === "branch" ? "BRANCH" : (loopable ? "LOOP" : "AGENT"));
      const kind = svgEl("text", { x: 151, y: 27, "text-anchor": "middle", class: "node-kind" }, kindText);
      const titleX = nodeType === "condition" ? 90 : 12;
      const titleAnchor = nodeType === "condition" ? "middle" : "start";
      const title = svgEl("text", { x: titleX, y: 51, "text-anchor": titleAnchor, class: "node-title" }, truncate(node.label || node.id, nodeType === "condition" ? 17 : 20));
      const time = node.planned_duration_seconds ? `${Math.round(node.elapsed_seconds || 0)}/${Math.round(node.planned_duration_seconds)}s` : "-";
      const backend = node.backend || "simulator";
      const visits = node.max_visits && node.max_visits > 1 ? ` | v ${node.visits || 0}/${node.max_visits}` : "";
      const metaX = nodeType === "condition" ? 90 : 12;
      const metaAnchor = nodeType === "condition" ? "middle" : "start";
      const meta = svgEl("text", { x: metaX, y: 68, "text-anchor": metaAnchor, class: "node-meta" }, truncate(`${backend} | hb ${node.heartbeat_count || 0} | ${time}${visits}`, nodeType === "condition" ? 22 : 31));
      g.append(shape);
      if (ring) g.appendChild(ring);
      g.append(badgeBg, badge, kind, title, meta);
      return g;
    }

    function nodeKind(node, visualInfo) {
      const type = String(node.type || "").toLowerCase();
      if (["condition", "decision", "router", "branch"].includes(type) || String(node.operation || "").toLowerCase() === "condition") {
        return "condition";
      }
      if (visualInfo.branchNodeIds.has(node.id)) return "branch";
      return "agent";
    }

    function updateSidebar(task) {
      const nodes = task.state.nodes || [];
      setMetrics(nodes);
      renderControlFlowSummary(task.state || {});
      const selected = selectedNodeId && nodes.find(node => node.id === selectedNodeId);
      if (selected) updateNodeDetail(selected);
      if (!selected) {
        document.getElementById("nodeDetail").textContent = "Select a node in the graph.";
        document.getElementById("agentInput").textContent = "Select a Multica node to inspect the injected Agent input.";
        document.getElementById("agentFinalOutput").textContent = "Select a Multica node to inspect the final Agent output.";
        document.getElementById("agentTrace").textContent = "Select a Multica node to inspect Agent execution.";
      }
      if (selectedNodeId && !selected) {
        selectedNodeId = null;
      }
      if (task.result) {
        document.getElementById("resultBox").textContent = formatResult(task.result);
      } else if (task.error) {
        document.getElementById("resultBox").textContent = task.error;
      } else {
        document.getElementById("resultBox").textContent = `Task ${task.status}`;
      }
    }

    function renderControlFlowSummary(state) {
      const summaryEl = document.getElementById("controlFlowSummary");
      const nodes = state.nodes || [];
      const edges = state.edges || [];
      const levelIndex = {};
      (state.levels || []).forEach((level, index) => {
        for (const nodeId of level) levelIndex[nodeId] = index;
      });
      const branchEdges = edges.filter(edge => edge.when);
      const loopEdges = edges.filter(edge => {
        const fromLevel = levelIndex[edge.from] ?? 0;
        const toLevel = levelIndex[edge.to] ?? 0;
        return toLevel <= fromLevel;
      });
      const loopNodeIds = new Set(loopEdges.flatMap(edge => [edge.from, edge.to]));
      const loopNodes = nodes.filter(node => Number(node.max_visits || 1) > 1 || Number(node.visits || 0) > 1 || loopNodeIds.has(node.id));
      if (!branchEdges.length && !loopEdges.length && !loopNodes.length) {
        summaryEl.innerHTML = '<div class="trace-empty">No branch or loop state in this graph.</div>';
        return;
      }
      const rows = [];
      for (const edge of branchEdges) {
        rows.push(`
          <div class="cf-row">
            <div><span class="cf-kind branch">BRANCH</span></div>
            <strong>${escapeHtml(edge.from)} -> ${escapeHtml(edge.to)}</strong>
            <span>${escapeHtml(edge.label || edge.when || "-")}</span>
            <span>taken ${escapeHtml(edge.taken_count || 0)} / skipped ${escapeHtml(edge.skipped_count || 0)}</span>
          </div>
        `);
      }
      for (const edge of loopEdges) {
        rows.push(`
          <div class="cf-row">
            <div><span class="cf-kind loop">LOOP EDGE</span></div>
            <strong>${escapeHtml(edge.from)} -> ${escapeHtml(edge.to)}</strong>
            <span>${escapeHtml(edge.label || edge.when || "-")}</span>
            <span>loop traversed ${escapeHtml(edge.taken_count || 0)} time(s)</span>
          </div>
        `);
      }
      for (const node of loopNodes) {
        rows.push(`
          <div class="cf-row">
            <div><span class="cf-kind loop">VISITS</span></div>
            <strong>${escapeHtml(node.id)}</strong>
            <span>${escapeHtml(node.visits || 0)} / ${escapeHtml(node.max_visits || 1)} visit(s)</span>
            <span>${escapeHtml(node.current_instance_id || "no active instance")}</span>
          </div>
        `);
      }
      summaryEl.innerHTML = rows.join("");
    }

    function formatResult(result) {
      const text = safeStringify(result);
      const limit = 24000;
      if (text.length <= limit) return text;
      return `${text.slice(0, limit)}\\n...<truncated for UI>`;
    }

    function safeStringify(value) {
      const seen = new WeakSet();
      return JSON.stringify(value, (key, item) => {
        if (typeof item === "string" && item.length > 2000) {
          return `${item.slice(0, 2000)}...<truncated>`;
        }
        if (Array.isArray(item) && item.length > 10) {
          return item.slice(-10);
        }
        if (item && typeof item === "object") {
          if (seen.has(item)) return "[Circular]";
          seen.add(item);
        }
        return item;
      }, 2);
    }

    function setMetrics(nodes) {
      setText("completedCount", nodes.filter(node => node.status === "completed").length);
      setText("runningCount", nodes.filter(node => node.status === "running").length);
      setText("suspendedCount", nodes.filter(node => node.status === "suspended").length);
      setText("pendingCount", nodes.filter(node => node.status === "pending").length);
      setText("failedCount", nodes.filter(node => node.status === "failed").length);
    }

    function updateNodeDetail(node) {
      const deps = node.deps && node.deps.length ? node.deps.join(", ") : "-";
      const instances = Array.isArray(node.instances) && node.instances.length
        ? node.instances.map(item => `${item.id}:${item.status}`).join(", ")
        : "-";
      const planned = node.planned_duration_seconds == null ? "-" : `${node.planned_duration_seconds.toFixed(2)}s`;
      const elapsed = node.elapsed_seconds == null ? "-" : `${node.elapsed_seconds.toFixed(2)}s`;
      const detailEl = document.getElementById("nodeDetail");
      detailEl.innerHTML = `
        <dl>
          <dt>ID</dt><dd>${escapeHtml(node.id)}</dd>
          <dt>Status</dt><dd>${escapeHtml(node.status)}</dd>
          <dt>Type</dt><dd>${escapeHtml(node.type || "agent")}</dd>
          <dt>Operation</dt><dd>${escapeHtml(node.operation)}</dd>
          <dt>Deps</dt><dd>${escapeHtml(deps)}</dd>
          <dt>Join</dt><dd>${escapeHtml(node.join || "-")}</dd>
          <dt>Visits</dt><dd>${escapeHtml(node.visits || 0)} / ${escapeHtml(node.max_visits || 1)}</dd>
          <dt>Instances</dt><dd>${escapeHtml(instances)}</dd>
          <dt>Backend</dt><dd>${escapeHtml(node.backend || "simulator")}</dd>
          <dt>Agent</dt><dd>${escapeHtml(node.agent_name || "-")}</dd>
          <dt>Agent Status</dt><dd>${escapeHtml(node.agent_status || "-")}</dd>
          <dt>A2A State</dt><dd>${escapeHtml(node.a2a_state || "-")}</dd>
          <dt>A2A Task</dt><dd>${escapeHtml(node.a2a_task_id || "-")}</dd>
          <dt>Simulator Job</dt><dd>${escapeHtml(node.simulator_job_id || "-")}</dd>
          <dt>Multica Task</dt><dd>${escapeHtml(node.agent_service_task_id || "-")}</dd>
          <dt>Multica Job</dt><dd>${escapeHtml(node.multica_job_id || "-")}</dd>
          <dt>Hermes Job</dt><dd>${escapeHtml(node.hermes_job_id || "-")}</dd>
          <dt>Heartbeat</dt><dd>${escapeHtml(node.heartbeat_count || 0)} @ ${escapeHtml(node.last_heartbeat_at || "-")}</dd>
          <dt>Elapsed</dt><dd>${escapeHtml(elapsed)} / ${escapeHtml(planned)}</dd>
          <dt>Summary</dt><dd>${escapeHtml(node.summary || "-")}</dd>
        </dl>
        ${renderVisitHistory(node)}
      `;
      bindVisitHistoryActions(detailEl);
      updateAgentTrace(node);
    }

    function renderVisitHistory(node) {
      const nodeInstances = Array.isArray(node.instances) ? node.instances : [];
      if (!nodeInstances.length) {
        return `<div class="visit-history"><div class="trace-empty">No visit history for this node yet.</div></div>`;
      }
      const instanceResults = currentInstanceResults();
      return `
        <div class="visit-history">
          ${nodeInstances.map(instance => renderVisitCard(node, instance, instanceResults)).join("")}
        </div>
      `;
    }

    function currentInstanceResults() {
      const task = activeTask || {};
      return {
        ...(((task.result || {}).instance_results) || {}),
        ...(((task.state || {}).instance_results) || {}),
      };
    }

    function currentNodeResult(node) {
      const task = activeTask || {};
      const nodeId = typeof node === "string" ? node : node.id;
      const directResult = (((task.result || {}).results || {})[nodeId])
        || (((task.state || {}).results || {})[nodeId]);
      if (directResult) return directResult;

      const instanceResults = currentInstanceResults();
      if (instanceResults[nodeId]) return instanceResults[nodeId];
      if (node.current_instance_id && instanceResults[node.current_instance_id]) {
        return instanceResults[node.current_instance_id];
      }
      const nodeInstances = Array.isArray(node.instances) ? node.instances : [];
      for (let index = nodeInstances.length - 1; index >= 0; index -= 1) {
        const instance = nodeInstances[index];
        if (instance && instance.id && instanceResults[instance.id]) {
          return instanceResults[instance.id];
        }
      }
      return null;
    }

    function renderVisitCard(node, instance, instanceResults) {
      const result = instanceResults[instance.id] || null;
      const agentTaskId = instance.agent_service_task_id || (result || {}).agent_service_task_id || "";
      const multicaJobId = instance.multica_job_id || (result || {}).multica_job_id || "";
      const backend = String(instance.backend || node.backend || "").toLowerCase();
      const elapsed = instance.elapsed_seconds == null ? "-" : `${Number(instance.elapsed_seconds).toFixed(2)}s`;
      const active = instance.id === node.current_instance_id ? " active" : "";
      const payloadHtml = result
        ? `<details class="visit-payload"><summary>Visit payload</summary><pre>${escapeHtml(safeStringify(result))}</pre></details>`
        : `<div class="visit-meta">Payload is not available yet. For completed workflows, older visit payloads are also in Workflow Result -> instance_results.</div>`;
      const inspectButton = backend === "multica" && agentTaskId
        ? `<button class="visit-button" type="button" data-agent-task-id="${escapeHtml(agentTaskId)}" data-visit-id="${escapeHtml(instance.id)}">Inspect visit</button>`
        : "";
      return `
        <div class="visit-card${active}">
          <div class="visit-head">
            <div>
              <div class="visit-title">${escapeHtml(instance.id || "-")} · visit ${escapeHtml(instance.visit || "-")}</div>
              <div class="visit-meta">${escapeHtml(instance.status || "-")} · ${escapeHtml(instance.kind || "agent")} · ${escapeHtml(backend || "-")} · elapsed ${escapeHtml(elapsed)}</div>
            </div>
            <span class="trace-badge">${escapeHtml(instance.agent_status || instance.status || "-")}</span>
          </div>
          <div class="visit-meta">
            Started: ${escapeHtml(instance.started_at || "-")}<br>
            Finished: ${escapeHtml(instance.finished_at || "-")}<br>
            A2A: ${escapeHtml(instance.a2a_task_id || "-")}<br>
            Multica: ${escapeHtml(agentTaskId || "-")}<br>
            Multica Job: ${escapeHtml(multicaJobId || "-")}<br>
            Hermes: ${escapeHtml(instance.hermes_job_id || (result || {}).hermes_job_id || "-")}<br>
            ${escapeHtml(instance.summary || "-")}
          </div>
          <div class="visit-actions">${inspectButton}</div>
          ${payloadHtml}
        </div>
      `;
    }

    function renderHermesPanels(node, result) {
      const inputEl = document.getElementById("agentInput");
      const outputEl = document.getElementById("agentFinalOutput");
      const traceEl = document.getElementById("agentTrace");
      inputEl.innerHTML = node.hermes_prompt
        ? renderInputSection("Hermes Prompt", node.hermes_prompt)
        : `<div class="trace-empty">Hermes prompt is not available yet.</div>`;
      const output = result && (result.latest_comment || result.stdout);
          outputEl.innerHTML = output
        ? renderFinalOutput({
            content: output,
            label: "job",
            id: result.hermes_job_id || node.hermes_job_id,
            chars: String(output).length,
            approx_tokens: Math.ceil(String(output).length / 4),
          })
        : `<div class="trace-empty">Hermes final output is not available yet.</div>`;
      traceEl.innerHTML = renderHermesTrace(node, result);
    }

    function renderHermesTrace(node, result) {
      const trace = (result && Array.isArray(result.trace)) ? result.trace : [];
      const header = `
        <div class="trace-summary">
          <div class="trace-card"><strong>${escapeHtml(node.status || "-")}</strong><span>Node status</span></div>
          <div class="trace-card"><strong>${escapeHtml(trace.length)}</strong><span>Runtime events</span></div>
          <div class="trace-card"><strong>${escapeHtml(node.heartbeat_count || 0)}</strong><span>Heartbeats</span></div>
          <div class="trace-card"><strong>${escapeHtml(node.elapsed_seconds || 0)}s</strong><span>Elapsed</span></div>
        </div>
        <div class="trace-meta">
          Hermes Job: ${escapeHtml(node.hermes_job_id || (result || {}).hermes_job_id || "-")}<br>
          Command: ${escapeHtml((result || {}).hermes_command || "-")}<br>
          Workdir: ${escapeHtml((result || {}).hermes_workdir || "-")}
        </div>
      `;
      if (!trace.length) {
        return `${header}<div class="trace-empty">Hermes runtime events are not available yet.</div>`;
      }
      return `${header}<div class="trace-list">${trace.map((event, index) => renderTraceStep({
        kind: event.type || "runtime",
        title: event.title || event.type || "Hermes runtime event",
        description: event.description || "Direct Hermes provider event.",
        start_seq: index + 1,
        end_seq: index + 1,
        created_at: event.timestamp,
        time_to_next_seconds: event.duration_seconds,
        preview: safeStringify(event),
        chars: safeStringify(event).length,
        approx_tokens: Math.ceil(safeStringify(event).length / 4),
      })).join("")}</div>`;
    }

    function renderMulticaJobPanels(node, result) {
      const inputEl = document.getElementById("agentInput");
      const outputEl = document.getElementById("agentFinalOutput");
      const traceEl = document.getElementById("agentTrace");
      inputEl.innerHTML = renderMulticaJobInput(node);
      const output = result && (result.final_reply || result.result || result.latest_comment);
      outputEl.innerHTML = output
        ? renderFinalOutput({
            content: output,
            label: "job",
            id: result.multica_job_id || node.multica_job_id,
            created_at: result.completed_at,
            chars: String(output).length,
            approx_tokens: Math.ceil(String(output).length / 4),
          })
        : `<div class="trace-empty">Multica native job final output is not available yet.</div>`;
      traceEl.innerHTML = renderMulticaJobTrace(node, result);
    }

    function renderMulticaJobInput(node) {
      const input = node.agent_input_payload || {};
      const nodeInput = input.node || {};
      const dependencyArtifacts = input.dependency_artifacts || [];
      const graphInput = input.graph_input || {};
      const controlFlow = input.control_flow || {};
      const instruction = ((nodeInput.agent || {}).prompt || node.prompt || node.description || "-");
      return `
        <div class="trace-meta">
          Multica Job: ${escapeHtml(node.multica_job_id || "-")}<br>
          Node: ${escapeHtml(node.id || "-")}<br>
          Dependency artifacts: ${escapeHtml(dependencyArtifacts.length || 0)}<br>
          Graph input keys: ${escapeHtml(Object.keys(graphInput).join(", ") || "-")}
        </div>
        ${renderInputSection("Node Instruction", instruction)}
        ${renderInputSection("Graph Input", safeStringify(graphInput))}
        ${renderInputSection("Dependency Artifacts", safeStringify(dependencyArtifacts))}
        ${renderInputSection("Control Flow", safeStringify(controlFlow))}
      `;
    }

    function renderMulticaJobTrace(node, result) {
      const messages = (result && Array.isArray(result.messages)) ? result.messages : [];
      const header = `
        <div class="trace-summary">
          <div class="trace-card"><strong>${escapeHtml(node.agent_status || node.status || "-")}</strong><span>Job status</span></div>
          <div class="trace-card"><strong>${escapeHtml(messages.length)}</strong><span>Messages</span></div>
          <div class="trace-card"><strong>${escapeHtml(node.heartbeat_count || 0)}</strong><span>Polls</span></div>
          <div class="trace-card"><strong>${escapeHtml(node.elapsed_seconds || 0)}s</strong><span>Elapsed</span></div>
        </div>
        <div class="trace-meta">
          Multica Job: ${escapeHtml(node.multica_job_id || (result || {}).multica_job_id || "-")}<br>
          Created: ${escapeHtml(formatDateTime((result || {}).created_at))}<br>
          Started: ${escapeHtml(formatDateTime((result || {}).started_at))}<br>
          Completed: ${escapeHtml(formatDateTime((result || {}).completed_at))}
        </div>
      `;
      if (!messages.length) {
        return `${header}<div class="trace-empty">No Multica native job messages are available yet.</div>`;
      }
      return `${header}<div class="trace-list">${messages.map(message => renderTraceStep({
        kind: "agent_text",
        title: message.role || message.type || "Native job message",
        description: "Message returned by Multica Job API execution.",
        start_seq: message.sequence || message.seq,
        end_seq: message.sequence || message.seq,
        created_at: message.created_at,
        preview: message.text || message.content || safeStringify(message),
        chars: String(message.text || message.content || "").length,
        approx_tokens: Math.ceil(String(message.text || message.content || "").length / 4),
      })).join("")}</div>`;
    }

    function renderTraceStep(step) {
      const type = String(step.kind || step.type || "message");
      const cssType = type === "tool_use" || type === "tool_result" || type === "text" || type === "agent_text" ? (type === "agent_text" ? "text" : type) : "message";
      const seq = step.start_seq === step.end_seq ? `#${step.start_seq ?? "-"}` : `#${step.start_seq ?? "-"}-${step.end_seq ?? "-"}`;
      const label = step.tool ? `${seq} ${step.tool}` : seq;
      const status = step.status ? ` | ${step.status}` : "";
      const exitCode = step.exit_code == null ? "" : ` | exit ${step.exit_code}`;
      return `
        <div class="trace-step ${escapeHtml(cssType)}">
          <div class="trace-step-header">
            <span class="trace-badge">${escapeHtml(label)}</span>
            <span class="trace-time">${escapeHtml(formatDateTime(step.created_at))}</span>
          </div>
          <div class="trace-step-title">
            ${escapeHtml(step.title || "Agent 执行步骤")}
            ${step.inferred_from_neighbor ? '<span class="trace-inference-badge">基于相邻步骤推断</span>' : ''}
          </div>
          <div class="trace-step-desc">${escapeHtml(step.description || "该步骤来自 Agent 执行轨迹。")}</div>
          ${renderTraceDuration(step)}
          <div class="trace-raw-label">原始记录</div>
          <pre class="trace-preview">${escapeHtml(step.preview || "")}</pre>
          <div class="trace-counts">${escapeHtml(step.chars || 0)} chars | ~${escapeHtml(step.approx_tokens || 0)} tokens${escapeHtml(status)}${escapeHtml(exitCode)}</div>
        </div>
      `;
    }

    function statusLabel(status) {
      return {
        pending: "WAIT",
        running: "RUN",
        suspended: "SLEEP",
        completed: "DONE",
        failed: "FAIL",
      }[status] || status.toUpperCase();
    }

    function svgEl(tag, attrs, text) {
      const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
      for (const [key, value] of Object.entries(attrs)) el.setAttribute(key, value);
      if (text !== undefined) el.textContent = text;
      return el;
    }

    
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
