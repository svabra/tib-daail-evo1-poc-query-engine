(function () {
  var FLOW_LABEL_START = 0.12;
  var FLOW_LABEL_END = 0.88;
  var FLOW_LABEL_GAP_FROM_LINE = 16;
  var FLOW_LABEL_SAMPLE_DISTANCE = 2;
  var FLOW_LABEL_STAGGER = 0.14;

  function parseDurationMs(value) {
    var text = String(value || "").trim();
    var secondsMatch = text.match(/^([0-9]+(?:\.[0-9]+)?)s$/);
    var millisecondsMatch = text.match(/^([0-9]+(?:\.[0-9]+)?)ms$/);

    if (secondsMatch) {
      return Number(secondsMatch[1]) * 1000;
    }
    if (millisecondsMatch) {
      return Number(millisecondsMatch[1]);
    }
    return 4000;
  }

  function normalizeLabelAngle(angle) {
    var readableAngle = angle;

    while (readableAngle > 180) {
      readableAngle -= 360;
    }
    while (readableAngle < -180) {
      readableAngle += 360;
    }
    if (readableAngle > 90) {
      readableAngle -= 180;
    }
    if (readableAngle < -90) {
      readableAngle += 180;
    }
    return readableAngle;
  }

  function flowLabelOpacity(progress) {
    if (progress < 0.14) {
      return progress / 0.14;
    }
    if (progress > 0.78) {
      return Math.max(0, (0.94 - progress) / 0.16);
    }
    return 1;
  }

  function setupFlowState(flow) {
    var path = flow.querySelector("path");
    var label = flow.querySelector("[data-flow-label]");

    if (!path || !label || typeof path.getTotalLength !== "function") {
      return null;
    }

    return {
      flow: flow,
      path: path,
      label: label,
      durationMs: parseDurationMs(flow.dataset.flowDuration),
      length: path.getTotalLength(),
      startedAt: 0,
    };
  }

  function renderFlowLabel(state, timestamp, staticProgress) {
    var elapsed = Math.max(0, timestamp - state.startedAt);
    var progress =
      typeof staticProgress === "number"
        ? staticProgress
        : (elapsed % state.durationMs) / state.durationMs;
    var pathProgress =
      FLOW_LABEL_START + (FLOW_LABEL_END - FLOW_LABEL_START) * progress;
    var distance = state.length * pathProgress;
    var before = state.path.getPointAtLength(
      Math.max(0, distance - FLOW_LABEL_SAMPLE_DISTANCE)
    );
    var after = state.path.getPointAtLength(
      Math.min(state.length, distance + FLOW_LABEL_SAMPLE_DISTANCE)
    );
    var point = state.path.getPointAtLength(distance);
    var tangent = Math.atan2(after.y - before.y, after.x - before.x);
    var angle = normalizeLabelAngle((tangent * 180) / Math.PI);
    var normalX = -Math.sin(tangent);
    var normalY = Math.cos(tangent);
    var x = point.x + normalX * FLOW_LABEL_GAP_FROM_LINE;
    var y = point.y + normalY * FLOW_LABEL_GAP_FROM_LINE;

    state.label.setAttribute(
      "transform",
      "translate(" +
        x.toFixed(2) +
        " " +
        y.toFixed(2) +
        ") rotate(" +
        angle.toFixed(2) +
        ")"
    );
    state.label.style.opacity =
      typeof staticProgress === "number"
        ? "1"
        : flowLabelOpacity(progress).toFixed(3);
  }

  function hideFlowLabel(state) {
    state.label.style.opacity = "0";
  }

  function prefersReducedMotion() {
    return (
      typeof window.matchMedia === "function" &&
      window.matchMedia("(prefers-reduced-motion: reduce)").matches
    );
  }

  function setupOverviewMap(map) {
    var flowElements = Array.prototype.slice.call(
      map.querySelectorAll("[data-topic-use-case-flow]")
    );
    var flowStates = flowElements.map(setupFlowState).filter(Boolean);
    var bubbles = Array.prototype.slice.call(map.querySelectorAll("[data-topic-slug]"));
    var animationHandle = null;

    function animateTopicUseCases(timestamp) {
      var hasVisibleFlow = false;

      flowStates.forEach(function (state) {
        if (state.flow.classList.contains("is-visible")) {
          hasVisibleFlow = true;
          renderFlowLabel(state, timestamp);
        } else {
          hideFlowLabel(state);
        }
      });

      animationHandle = hasVisibleFlow
        ? window.requestAnimationFrame(animateTopicUseCases)
        : null;
    }

    function startAnimation() {
      if (!animationHandle && !prefersReducedMotion()) {
        animationHandle = window.requestAnimationFrame(animateTopicUseCases);
      }
    }

    function clear() {
      flowStates.forEach(function (state) {
        state.flow.classList.remove("is-visible");
        hideFlowLabel(state);
      });
      if (animationHandle) {
        window.cancelAnimationFrame(animationHandle);
        animationHandle = null;
      }
    }

    function showOutgoing(topicSlug) {
      var timestamp =
        window.performance && typeof window.performance.now === "function"
          ? window.performance.now()
          : Date.now();
      var hasVisibleFlow = false;
      var visibleIndex = 0;

      flowStates.forEach(function (state) {
        var isVisible = state.flow.dataset.sourceTopic === topicSlug;

        state.flow.classList.toggle("is-visible", isVisible);
        if (isVisible) {
          var labelStagger = visibleIndex * FLOW_LABEL_STAGGER;

          visibleIndex += 1;
          hasVisibleFlow = true;
          state.startedAt = timestamp - state.durationMs * labelStagger;
          renderFlowLabel(
            state,
            timestamp,
            prefersReducedMotion() ? 0.5 + labelStagger : undefined
          );
        } else {
          hideFlowLabel(state);
        }
      });

      if (hasVisibleFlow) {
        startAnimation();
      }
    }

    bubbles.forEach(function (bubble) {
      bubble.addEventListener("pointerenter", function () {
        showOutgoing(bubble.dataset.topicSlug);
      });
      bubble.addEventListener("focusin", function () {
        showOutgoing(bubble.dataset.topicSlug);
      });
    });

    map.addEventListener("pointerleave", clear);
    map.addEventListener("focusout", function (event) {
      if (!map.contains(event.relatedTarget)) {
        clear();
      }
    });
  }

  function nodeKey(type, id) {
    return type + ":" + id;
  }

  function centerY(rect, baseRect) {
    return rect.top - baseRect.top + rect.height / 2;
  }

  function drawPath(svg, boardRect, capability, service) {
    var capRect = capability.getBoundingClientRect();
    var serviceRect = service.getBoundingClientRect();
    var startX = capRect.right - boardRect.left;
    var startY = centerY(capRect, boardRect);
    var endX = serviceRect.left - boardRect.left;
    var endY = centerY(serviceRect, boardRect);
    var midX = startX + (endX - startX) / 2;
    var path = document.createElementNS("http://www.w3.org/2000/svg", "path");

    path.setAttribute(
      "d",
      "M " +
        startX.toFixed(1) +
        " " +
        startY.toFixed(1) +
        " C " +
        midX.toFixed(1) +
        " " +
        startY.toFixed(1) +
        " " +
        midX.toFixed(1) +
        " " +
        endY.toFixed(1) +
        " " +
        endX.toFixed(1) +
        " " +
        endY.toFixed(1)
    );
    path.setAttribute("class", "data-platform-topic-connection");
    svg.appendChild(path);
  }

  function setupTopicBoard(board) {
    var svg = board.querySelector("[data-data-platform-topic-connections]");
    var linkElements = Array.prototype.slice.call(
      board.querySelectorAll("[data-platform-topic-link]")
    );
    var nodes = Array.prototype.slice.call(
      board.querySelectorAll("[data-node-type][data-node-id]")
    );
    var nodeMap = new Map();
    var activeNode = null;

    nodes.forEach(function (node) {
      nodeMap.set(nodeKey(node.dataset.nodeType, node.dataset.nodeId), node);
    });

    var links = linkElements.map(function (link) {
      return {
        capability: link.dataset.capability,
        service: link.dataset.service,
      };
    });

    function clear() {
      activeNode = null;
      svg.replaceChildren();
      board.classList.remove("is-linking");
      nodes.forEach(function (node) {
        node.classList.remove("is-active", "is-related");
      });
    }

    function renderFor(node) {
      var type = node.dataset.nodeType;
      var id = node.dataset.nodeId;
      var boardRect = board.getBoundingClientRect();
      var matchedLinks = links.filter(function (link) {
        return type === "capability" ? link.capability === id : link.service === id;
      });

      activeNode = node;
      svg.setAttribute("viewBox", "0 0 " + boardRect.width + " " + boardRect.height);
      svg.replaceChildren();
      board.classList.toggle("is-linking", matchedLinks.length > 0);
      nodes.forEach(function (candidate) {
        candidate.classList.remove("is-active", "is-related");
      });
      node.classList.add("is-active");

      matchedLinks.forEach(function (link) {
        var capability = nodeMap.get(nodeKey("capability", link.capability));
        var service = nodeMap.get(nodeKey("service", link.service));

        if (!capability || !service) {
          return;
        }

        capability.classList.add("is-related");
        service.classList.add("is-related");
        drawPath(svg, boardRect, capability, service);
      });
    }

    nodes.forEach(function (node) {
      node.addEventListener("pointerenter", function () {
        renderFor(node);
      });
      node.addEventListener("focus", function () {
        renderFor(node);
      });
    });

    board.addEventListener("pointerleave", clear);
    board.addEventListener("focusout", function (event) {
      if (!board.contains(event.relatedTarget)) {
        clear();
      }
    });

    window.addEventListener("resize", function () {
      if (activeNode) {
        renderFor(activeNode);
      }
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    Array.prototype.slice
      .call(document.querySelectorAll("[data-data-platform-overview-map]"))
      .forEach(setupOverviewMap);

    Array.prototype.slice
      .call(document.querySelectorAll("[data-data-platform-topic-board]"))
      .forEach(setupTopicBoard);
  });
})();
