export class ComplaintEngineClient {
  constructor({ baseUrl, apiKey }) {
    this.baseUrl = String(baseUrl || "").replace(/\/+$/, "");
    this.apiKey = String(apiKey || "").trim();
  }

  async request(path, options = {}) {
    const res = await fetch(`${this.baseUrl}${path}`, {
      ...options,
      headers: {
        Authorization: `Bearer ${this.apiKey}`,
        "Content-Type": "application/json",
        ...(options.headers || {}),
      },
    });
    const contentType = res.headers.get("content-type") || "";
    const body = contentType.includes("application/json") ? await res.json() : await res.text();
    if (!res.ok) {
      const detail = body && typeof body === "object" ? body.detail || body.message : body;
      throw new Error(String(detail || `HTTP ${res.status}`));
    }
    return body;
  }

  createComplaint(data) {
    return this.request("/complaints", {
      method: "POST",
      body: JSON.stringify(data),
    });
  }

  listComplaints(params = {}) {
    const query = new URLSearchParams(params).toString();
    return this.request(`/complaints${query ? `?${query}` : ""}`);
  }

  updateComplaint(id, data) {
    return this.request(`/complaints/${encodeURIComponent(id)}`, {
      method: "PUT",
      body: JSON.stringify(data),
    });
  }

  classify(text) {
    return this.request("/ai/classify", {
      method: "POST",
      body: JSON.stringify({ text }),
    });
  }

  generateDailyReport(params = {}) {
    const query = new URLSearchParams(params).toString();
    return this.request(`/report/daily${query ? `?${query}` : ""}`);
  }
}

// Example:
// const client = new ComplaintEngineClient({
//   baseUrl: "https://your-service.example.com/api",
//   apiKey: "sk-ka-...",
// });
// await client.createComplaint({
//   building: "101",
//   unit: "1203",
//   channel: "전화",
//   content: "엘리베이터가 멈췄어요",
// });
