function authHeaders() {
  const k = $("#apiKeyInput").val().trim();
  return k ? { "X-API-Key": k } : {};
}

function showAuthOrFetchError(xhr, context) {
  if (xhr.status === 401) {
    $("#authNotice").removeClass("hidden").text("Unauthorized (401): Enter your configured INGEST_API_KEY above to query protected endpoints.");
  } else {
    $("#authNotice").removeClass("hidden").text(context + " failed (HTTP " + xhr.status + ").");
  }
}

function refreshAll() {
  const headers = authHeaders();
  const service = $("#filterService").val().trim();
  const onlyAnom = $("#onlyAnomaly").is(":checked");

  $.ajax({ url: "/metrics", headers })
    .done((res) => {
      $("#authNotice").addClass("hidden");
      const p = res.pipeline || {};
      $("#kpiEnqueued").text(p.enqueued ?? 0);
      $("#kpiProcessed").text(p.processed ?? 0);
      $("#kpiStored").text(res.stored_events ?? 0);
      $("#kpiAnomalies").text(res.stored_anomalies ?? 0);
    })
    .fail((xhr) => {
      showAuthOrFetchError(xhr, "Metrics fetch");
    });

  const latUrl = "/v1/metrics/latency" + (service ? "?service=" + encodeURIComponent(service) : "");
  $.ajax({ url: latUrl, headers })
    .done((res) => {
      const p99Val = res.latency && typeof res.latency.p99_ms === "number" ? res.latency.p99_ms.toFixed(2) : "0.00";
      $("#kpiP99").text(p99Val);
    })
    .fail((xhr) => {
      $("#kpiP99").text("0.00");
      showAuthOrFetchError(xhr, "Latency summary fetch");
    });

  const logsUrl = `/v1/logs?limit=50&anomaly=${onlyAnom}` + (service ? "&service=" + encodeURIComponent(service) : "");
  $.ajax({ url: logsUrl, headers })
    .done((res) => {
      const $body = $("#logsBody").empty();
      (res.logs || []).forEach((ev) => {
        const $tr = $('<tr class="log-row"></tr>');
        $('<td class="mono muted"></td>').text(ev.timestamp || "").appendTo($tr);
        $('<td class="svc"></td>').text(ev.service || "").appendTo($tr);
        $('<td></td>').text(ev.level || "INFO").appendTo($tr);
        $('<td class="mono"></td>').text(typeof ev.latency_ms === "number" ? ev.latency_ms.toFixed(1) : "0").appendTo($tr);
        $('<td></td>').text(ev.message || "").appendTo($tr);
        $('<td></td>')
          .addClass(ev.is_anomaly ? "anom-yes" : "muted")
          .text(ev.is_anomaly ? (ev.anomaly_reason || "anomaly") : "normal")
          .appendTo($tr);
        $body.append($tr);
      });
    })
    .fail((xhr) => {
      $("#logsBody").empty();
      showAuthOrFetchError(xhr, "Logs query");
    });
}

$(function () {
  $("#refreshBtn, #onlyAnomaly").on("click change", refreshAll);
  $("#apiKeyInput").on("change", refreshAll);
  $("#apiKeyInput").on("keydown", function (e) {
    if (e.key === "Enter") {
      e.preventDefault();
      refreshAll();
    }
  });
  $("#filterService").on("input", refreshAll);
  $("#ingestForm").on("submit", function (e) {
    e.preventDefault();
    const payload = {
      service: $("#evService").val().trim(),
      level: $("#evLevel").val(),
      latency_ms: parseFloat($("#evLatency").val()) || 0,
      message: $("#evMessage").val().trim()
    };
    $.ajax({
      url: "/v1/ingest",
      method: "POST",
      contentType: "application/json",
      headers: authHeaders(),
      data: JSON.stringify(payload)
    })
      .done(() => {
        $("#ingestStatus").text("Accepted (202): Event queued into service FIFO shard.").removeClass("status-err").addClass("status-ok");
        setTimeout(refreshAll, 120);
      })
      .fail((xhr) => {
        $("#ingestStatus").text(`Error (${xhr.status}): ${xhr.responseText || "Failed"}`).removeClass("status-ok").addClass("status-err");
      });
  });
  refreshAll();
});
