(() => {
  const API = window.API_BASE || "http://127.0.0.1:8001"; // backend directo

  const form = document.getElementById("chatForm");
  const input = document.getElementById("userInput");
  const out = document.getElementById("chatOutput");
  const empty = document.getElementById("emptyState");
  const loader = document.getElementById("loader");
  const boot = document.getElementById("bootloader");

  function showLoader(on) { loader.classList.toggle("hidden", !on); }

  // ============ BOOT LOADER ============
  // Mantener visible al menos 900ms y hasta que /ping responda (o 3s fallback)
  const BOOT_MIN_MS = 900;
  const BOOT_MAX_MS = 3000;
  let bootStart = performance.now();

  async function hideBootWhenReady() {
    if (!boot) return;
    let pingOk = false;

    // Hacemos ping con timeout manual (3s)
    const ctrl = new AbortController();
    const tOut = setTimeout(() => ctrl.abort(), BOOT_MAX_MS);
    try {
      await fetch(`${API}/ping`, { cache: "no-store", signal: ctrl.signal });
      pingOk = true;
    } catch (_) {
      // no pasa nada; igual ocultaremos por timeout
    } finally {
      clearTimeout(tOut);
    }

    const elapsed = performance.now() - bootStart;
    const wait = Math.max(0, BOOT_MIN_MS - elapsed);

    setTimeout(() => {
      boot.classList.add("hide");             // aplica el fade-out (CSS)
      setTimeout(() => boot.remove(), 400);   // lo quita del DOM
      // opcional: podés mostrar un toast si pingOk === false
    }, wait);
  }

  // Arrancar la lógica del loader cuando TODA la página terminó de cargar
  if (document.readyState === "complete") {
    // si el script se inyecta al final y ya está completo, igual respetamos el mínimo
    setTimeout(hideBootWhenReady, 0);
  } else {
    window.addEventListener("load", () => {
      bootStart = performance.now(); // medimos desde que realmente se ve
      hideBootWhenReady();
    });
  }
  // ============ /BOOT LOADER ============

  function addMsg(role, html) {
    empty?.classList.add("hidden");

    const wrap = document.createElement("div");
    wrap.className = `message ${role} flex gap-2 items-start`;

    const who = document.createElement("div");
    who.className =
      "who px-2 py-1 rounded-[20px] font-semibold text-[11px] min-w-[56px] text-center border shrink-0";
    if (role === "user") {
      who.textContent = "Tú";
      who.className += " bg-blue-600/15 text-blue-300 border-blue-400/30";
    } else {
      who.textContent = "TELCO";
      who.className += " bg-emerald-600/15 text-emerald-300 border-emerald-400/30";
    }

    const bubble = document.createElement("div");
    bubble.className = "bubble flex-1 glass rounded-xl p-3 overflow-x-auto";
    bubble.innerHTML = html;

    wrap.appendChild(who);
    wrap.appendChild(bubble);
    out.appendChild(wrap);
    out.scrollTop = out.scrollHeight;
  }

  function addMd(role, mdText) {
    const card =
      `<div class="glass rounded-xl p-4 overflow-x-auto">
         <pre class="font-mono text-[13px] leading-6 whitespace-pre min-w-max">${mdText}</pre>
       </div>`;
    addMsg(role, card);
  }

  async function ask(q) {
    addMsg("user", q);
    showLoader(true);
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 15000); // 15s

    try {
      const resp = await fetch(`${API}/ask`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt: q }),
        signal: ctrl.signal,
      });

      let data;
      try { data = await resp.json(); } catch { data = { error: `Respuesta no-JSON (status ${resp.status})` }; }

      if (!resp.ok) {
        const msg = data?.error || data?.detail || `Error ${resp.status}`;
        addMsg("bot", `<p class='text-red-300'>${msg}</p>`);
        return;
      }

      if (data.md) {
        addMd("bot", data.md);
      } else {
        const text = data.html ?? data.answer ?? data;
        if (typeof text === "object") {
          addMd("bot", JSON.stringify(text, null, 2));
        } else {
          addMsg("bot", text);
        }
      }
    } catch (e) {
      const aborted = e?.name === "AbortError";
      addMsg("bot",
        `<p class='text-red-300'>${aborted ? "⏱️ Timeout de 15s" : "Fallo al consultar el backend"}. Probá de nuevo.</p>`
      );
      console.error(e);
    } finally {
      clearTimeout(t);
      showLoader(false);
    }
  }

  form.addEventListener("submit", (e) => {
    e.preventDefault();
    const q = (input.value || "").trim();
    if (!q) return;
    input.value = "";
    ask(q);
  });

  document.querySelectorAll(".quick").forEach(btn => {
    btn.addEventListener("click", () => ask(btn.dataset.q));
  });
})();
