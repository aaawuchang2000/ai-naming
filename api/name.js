// =============================================
// AI 起名 - Vercel Serverless 函数
// 调用豆包·火山方舟（OpenAI 兼容接口）流式生成起名方案，并保存到 Supabase
//
// 环境变量（在 Vercel 项目 Settings → Environment Variables 中配置，勿写死在代码里）：
//   ARK_API_KEY                必填  火山方舟 API Key（https://console.volcengine.com/ark）
//   MODEL_NAME                 可选  模型名或推理接入点 ID（默认 doubao-seed-1-6-250615，
//                                    如你的账号用的是接入点，可填 ep-xxxxxxxx）
//   SUPABASE_URL               必填  Supabase 项目 URL（如 https://xxxx.supabase.co）
//   SUPABASE_SERVICE_ROLE_KEY  必填  Supabase service_role 密钥（仅服务端使用，勿暴露给前端）
// =============================================

const ARK_BASE_URL = 'https://ark.cn-beijing.volces.com/api/v3';

module.exports = async function handler(req, res) {
  // ---- 1. 只接受 POST ----
  if (req.method !== 'POST') {
    return jsonError(res, 405, '仅支持 POST 请求');
  }

  // ---- 2. 读取并校验请求体 ----
  let body;
  try {
    body = await readJson(req);
  } catch {
    return jsonError(res, 400, '请求体不是合法的 JSON');
  }

  const requirement = typeof body.requirement === 'string' ? body.requirement.trim() : '';
  if (!requirement) {
    return jsonError(res, 400, '请输入起名需求');
  }
  if (requirement.length > 500) {
    return jsonError(res, 400, '起名需求请控制在 500 字以内');
  }

  // ---- 3. 读取环境变量（密钥全部来自环境变量） ----
  const apiKey = process.env.ARK_API_KEY;
  const supabaseUrl = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  const model = process.env.MODEL_NAME || 'doubao-seed-1-6-250615';

  const missing = [];
  if (!apiKey) missing.push('ARK_API_KEY');
  if (!supabaseUrl) missing.push('SUPABASE_URL');
  if (!supabaseKey) missing.push('SUPABASE_SERVICE_ROLE_KEY');
  if (missing.length > 0) {
    return jsonError(res, 500, `缺少环境变量：${missing.join(', ')}`);
  }

  // ---- 4. 调用火山方舟（OpenAI 兼容）流式接口 ----
  const messages = [
    { role: 'system', content: SYSTEM_PROMPT },
    { role: 'user', content: requirement }
  ];

  // 先返回 SSE 响应头，再开始转发流
  res.writeHead(200, {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    'X-Accel-Buffering': 'no'
  });

  let upstream;
  try {
    upstream = await fetch(`${ARK_BASE_URL}/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`
      },
      body: JSON.stringify({ model, messages, stream: true })
    });
  } catch (err) {
    sendEvent(res, { type: 'error', message: `连接模型服务失败：${err.message}` });
    res.end();
    return;
  }

  if (!upstream.ok) {
    const detail = await upstream.text().catch(() => '');
    sendEvent(res, { type: 'error', message: `模型接口返回 ${upstream.status}：${detail.slice(0, 300)}` });
    res.end();
    return;
  }

  // ---- 5. 逐块读取上游 SSE 流，抽取增量并转发给前端 ----
  const reader = upstream.body.getReader();
  const decoder = new TextDecoder('utf-8');
  let buffer = '';
  let fullText = '';
  let streamError = null;

  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      // SSE 事件以空行分隔
      const events = buffer.split('\n\n');
      buffer = events.pop();
      for (const evt of events) {
        const delta = parseSseDelta(evt);
        if (delta === null || delta === '[DONE]') continue;
        fullText += delta;
        sendEvent(res, { type: 'delta', content: delta });
      }
    }
  } catch (err) {
    streamError = err;
  }

  // 处理流末尾的残留数据
  if (buffer.trim()) {
    const delta = parseSseDelta(buffer);
    if (delta && delta !== '[DONE]') {
      fullText += delta;
      sendEvent(res, { type: 'delta', content: delta });
    }
  }

  // ---- 6. 把 需求 + 结果 + 模型 保存到 Supabase ----
  let saved = false;
  let saveError = null;
  if (!streamError && fullText.trim()) {
    try {
      const saveRes = await fetch(`${supabaseUrl}/rest/v1/ai_names`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          apikey: supabaseKey,
          Authorization: `Bearer ${supabaseKey}`,
          Prefer: 'return=minimal'
        },
        body: JSON.stringify({
          requirement,
          result: fullText.trim(),
          model
        })
      });
      if (saveRes.ok) {
        saved = true;
      } else {
        saveError = `Supabase 写入失败 HTTP ${saveRes.status}：${(await saveRes.text()).slice(0, 200)}`;
      }
    } catch (err) {
      saveError = `Supabase 写入异常：${err.message}`;
    }
  }

  // ---- 7. 发送收尾事件 ----
  if (streamError) {
    sendEvent(res, { type: 'error', message: `流式读取中断：${streamError.message}` });
  } else if (!fullText.trim()) {
    sendEvent(res, { type: 'error', message: '模型未返回内容，请检查 MODEL_NAME 是否已开通' });
  } else {
    sendEvent(res, { type: 'done', saved, model, saveError });
  }
  res.end();
};

// ============ 工具函数 ============

const SYSTEM_PROMPT = [
  '你是专业的中文起名专家。根据用户的起名需求，生成 3 组名字方案。',
  '要求：',
  '1. 每组包含：名字 + 寓意解释（解释 2~3 句，简洁明了）；',
  '2. 名字要贴合需求（姓氏、性别、风格、寓意、字辈、读音等），顺口好写、避免生僻字和负面谐音；',
  '3. 每组之间用空行分隔，格式为：**名字**（寓意：解释）。'
].join('\n');

function jsonError(res, status, message) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.end(JSON.stringify({ error: message }));
}

function sendEvent(res, obj) {
  res.write(`data: ${JSON.stringify(obj)}\n\n`);
}

// 从单个 SSE 事件文本中解析出 content 增量（火山方舟为 OpenAI 兼容格式）
function parseSseDelta(eventText) {
  let data = '';
  for (const line of eventText.split('\n')) {
    if (line.startsWith('data:')) data += line.slice(5).trim();
  }
  if (!data) return null;
  if (data === '[DONE]') return '[DONE]';
  try {
    const json = JSON.parse(data);
    const content = json.choices && json.choices[0] && json.choices[0].delta && json.choices[0].delta.content;
    return typeof content === 'string' ? content : null;
  } catch {
    return null;
  }
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', (chunk) => {
      raw += chunk;
      if (raw.length > 1024 * 1024) {
        req.destroy(new Error('请求体过大'));
      }
    });
    req.on('end', () => {
      try {
        resolve(JSON.parse(raw || '{}'));
      } catch (err) {
        reject(err);
      }
    });
    req.on('error', reject);
  });
}
