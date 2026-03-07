(function () {
  "use strict";

  const API = "/api";
  const loginScreen = document.getElementById("login-screen");
  const app = document.getElementById("app");
  const loginForm = document.getElementById("login-form");
  const loginError = document.getElementById("login-error");
  const userName = document.getElementById("user-name");
  const logoutBtn = document.getElementById("logout-btn");

  function api(path, options = {}) {
    const url = path.startsWith("http") ? path : API + path;
    return fetch(url, { credentials: "include", ...options }).then((r) => {
      if (r.status === 401) throw new Error("Unauthorized");
      return r;
    });
  }

  function apiJson(path, options = {}) {
    return api(path, options).then((r) => r.json());
  }

  function showLogin() {
    loginScreen.classList.remove("hidden");
    app.classList.add("hidden");
  }

  function showApp() {
    loginScreen.classList.add("hidden");
    app.classList.remove("hidden");
  }

  function checkAuth() {
    return apiJson("/me")
      .then((data) => {
        if (data.authenticated) {
          userName.textContent = data.username;
          showApp();
          loadAll();
        } else {
          showLogin();
        }
      })
      .catch(() => showLogin());
  }

  loginForm.addEventListener("submit", (e) => {
    e.preventDefault();
    loginError.textContent = "";
    const username = document.getElementById("login-username").value.trim();
    const password = document.getElementById("login-password").value;
    fetch(API + "/login", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    })
      .then((r) => r.json().then((data) => ({ ok: r.ok, data })))
      .then(({ ok, data }) => {
        if (ok) {
          userName.textContent = data.username;
          showApp();
          loadAll();
        } else {
          loginError.textContent = data.error || "Login failed";
        }
      })
      .catch(() => {
        loginError.textContent = "Network error";
      });
  });

  logoutBtn.addEventListener("click", () => {
    api("/logout", { method: "POST" }).then(() => {
      showLogin();
    });
  });

  // Tabs
  document.querySelectorAll("nav [data-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const id = "tab-" + btn.getAttribute("data-tab");
      document.querySelectorAll("nav button").forEach((b) => b.classList.remove("active"));
      document.querySelectorAll(".tab-panel").forEach((p) => p.classList.add("hidden"));
      btn.classList.add("active");
      const panel = document.getElementById(id);
      if (panel) panel.classList.remove("hidden");
    });
  });

  function loadAll() {
    loadInfo();
    loadConfig();
    loadLogs();
    loadServiceEnablement();
    loadFacts();
    loadSettings();
  }

  function loadInfo() {
    apiJson("/info")
      .then((data) => {
        const dl = document.getElementById("info-dl");
        dl.innerHTML = "";
        [
          ["Config path", data.config_path],
          ["DB path", data.db_path],
          ["Log path", data.log_path],
          ["Channels", (data.channels || []).join(", ") || "(none)"],
          ["Services", (data.services || []).join(", ") || "(none)"],
        ].forEach(([k, v]) => {
          dl.appendChild(Object.assign(document.createElement("dt"), { textContent: k }));
          dl.appendChild(Object.assign(document.createElement("dd"), { textContent: v }));
        });
      })
      .catch(() => {});
  }

  function loadConfig() {
    apiJson("/config")
      .then((data) => {
        document.getElementById("config-editor").value = JSON.stringify(data, null, 2);
      })
      .catch(() => {});
  }

  document.getElementById("config-save").addEventListener("click", () => {
    const msg = document.getElementById("config-message");
    let data;
    try {
      data = JSON.parse(document.getElementById("config-editor").value);
    } catch (e) {
      msg.textContent = "Invalid JSON: " + e.message;
      msg.className = "message err";
      return;
    }
    api("/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data),
    })
      .then((r) => (r.ok ? r.json() : r.json().then((d) => Promise.reject(d))))
      .then(() => {
        msg.textContent = "Saved.";
        msg.className = "message ok";
      })
      .catch((err) => {
        msg.textContent = err.error || "Save failed";
        msg.className = "message err";
      });
  });

  function loadLogs() {
    const tail = document.getElementById("logs-tail").value || 500;
    apiJson("/logs?tail=" + encodeURIComponent(tail))
      .then((data) => {
        document.getElementById("logs-content").textContent =
          data.lines && data.lines.length ? data.lines.join("\n") : "(no log lines)";
      })
      .catch(() => {});
  }

  document.getElementById("logs-refresh").addEventListener("click", loadLogs);

  function serviceId(name) {
    return name.split(".").pop() || name;
  }

  let serviceEnablementData = [];
  function loadServiceEnablement() {
    Promise.all([apiJson("/service_enablement"), apiJson("/info")])
      .then(([enablement, info]) => {
        serviceEnablementData = enablement.items || [];
        const channels = [...new Set(serviceEnablementData.map((i) => i.channel))];
        const cfgChannels = info.channels || [];
        channels.forEach((c) => {
          if (!cfgChannels.includes(c)) cfgChannels.push(c);
        });
        const configServices = info.services || [];
        const serviceIds = configServices.length
          ? [...new Set(configServices.map(serviceId))]
          : [...new Set(serviceEnablementData.map((i) => i.service))];
        serviceIds.sort();
        const container = document.getElementById("services-list");
        container.innerHTML = "";
        cfgChannels.sort().forEach((channel) => {
          const div = document.createElement("div");
          div.className = "service-row";
          const byChannel = serviceEnablementData.filter((i) => i.channel === channel);
          serviceIds.forEach((sid) => {
            const item = byChannel.find((i) => i.service === sid);
            const enabled = item ? item.enabled : false;
            const id = "svc-" + channel + "-" + sid;
            const label = document.createElement("label");
            label.innerHTML = `<input type="checkbox" id="${id}" data-channel="${channel}" data-service="${sid}" ${enabled ? "checked" : ""}> <span class="name">${sid}</span>`;
            div.appendChild(label);
          });
          const chLabel = document.createElement("span");
          chLabel.className = "channel";
          chLabel.textContent = channel;
          div.insertBefore(chLabel, div.firstChild);
          container.appendChild(div);
        });
      })
      .catch(() => {});
  }

  document.getElementById("services-list").addEventListener("change", (e) => {
    if (e.target.type !== "checkbox" || !e.target.dataset.channel) return;
    const channel = e.target.dataset.channel;
    const service = e.target.dataset.service;
    const enabled = e.target.checked;
    api("/service_enablement", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ channel, service, enabled }),
    })
      .then((r) => (r.ok ? null : r.json().then((d) => Promise.reject(d))))
      .then(() => {})
      .catch(() => {
        e.target.checked = !enabled;
      });
  });

  function loadFacts() {
    apiJson("/facts/categories")
      .then((data) => {
        document.getElementById("facts-summary").textContent =
          "Total facts: " + (data.total || 0);
        const ul = document.getElementById("facts-categories");
        ul.innerHTML = "";
        (data.categories || []).forEach((cat) => {
          const li = document.createElement("li");
          li.textContent = cat + ": " + (data.counts && data.counts[cat] != null ? data.counts[cat] : 0);
          ul.appendChild(li);
        });
      })
      .catch(() => {});
  }

  function loadSettings() {
    apiJson("/settings")
      .then((data) => {
        const tbody = document.querySelector("#settings-table tbody");
        tbody.innerHTML = "";
        (data.settings || []).forEach(({ key, value }) => {
          const tr = document.createElement("tr");
          tr.innerHTML = "<td>" + escapeHtml(key) + "</td><td>" + escapeHtml(value) + "</td>";
          tbody.appendChild(tr);
        });
      })
      .catch(() => {});
  }

  function escapeHtml(s) {
    const div = document.createElement("div");
    div.textContent = s;
    return div.innerHTML;
  }

  checkAuth();
})();
