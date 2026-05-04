const URL_RE = /https?:\/\/[^\s<>"')\]]+/gi;

const TRAILING_PUNCT_RE = /[.,;:!?，。！？、）)\]}]+$/;

function cleanUrl(rawUrl) {
  return String(rawUrl || '').trim().replace(TRAILING_PUNCT_RE, '');
}

function hostnameOf(url) {
  try {
    return new URL(url).hostname.replace(/^www\./i, '').toLowerCase();
  } catch {
    return '';
  }
}

function classifyUrl(url) {
  const host = hostnameOf(url);
  if (!host) return 'unknown';
  if (host === 'x.com' || host.endsWith('.x.com') || host === 'twitter.com' || host.endsWith('.twitter.com')) return 'x';
  if (host === 't.me' || host.endsWith('.t.me') || host.includes('telegram')) return 'telegram';
  if (host === 'github.com' || host.endsWith('.github.com')) return 'github';
  if (host.includes('discord')) return 'discord';
  if (host.includes('gmgn.ai')) return 'market';
  if (host.includes('dexscreener.com')) return 'market';
  if (host.includes('geckoterminal.com')) return 'market';
  if (host.includes('solscan.io') || host.includes('solana.fm')) return 'explorer';
  return 'website';
}

export function extractSignalLinks(text = '') {
  const seen = new Set();
  const links = [];
  for (const match of String(text || '').matchAll(URL_RE)) {
    const url = cleanUrl(match[0]);
    if (!url || seen.has(url)) continue;
    seen.add(url);
    links.push({
      url,
      type: classifyUrl(url),
      host: hostnameOf(url)
    });
  }
  return links;
}

function hasAny(text, patterns) {
  return patterns.some((pattern) => pattern.test(text));
}

const METRIC_LINE_RE = /^\s*(?:✡\s*)?(?:Super|AI|Trade|Security|Address|Sentiment|Media)\s+Index\b.*$/gim;
const ORGANIC_BUYERS_LINE_RE = /^\s*Organic\s+Buyers\b.*$/gim;

function stripSignalMetricLines(text = '') {
  return String(text || '')
    .replace(METRIC_LINE_RE, '')
    .replace(ORGANIC_BUYERS_LINE_RE, '');
}

export function scoreNarrativeFeatures(input = {}) {
  const description = input.description || '';
  const rawMessage = input.rawMessage || input.raw_message || '';
  const symbol = input.symbol || '';
  const name = input.name || '';
  const narrativeText = [symbol, name, description, stripSignalMetricLines(rawMessage)].filter(Boolean).join('\n');
  const text = [symbol, name, description, rawMessage].filter(Boolean).join('\n');
  const lower = text.toLowerCase();
  const narrativeLower = narrativeText.toLowerCase();
  const links = extractSignalLinks(text);
  const counts = links.reduce((acc, link) => {
    acc[link.type] = (acc[link.type] || 0) + 1;
    return acc;
  }, {});

  const tags = [];
  const reasons = [];
  let score = 0;

  const add = (points, tag, reason) => {
    score += points;
    if (tag && !tags.includes(tag)) tags.push(tag);
    if (reason) reasons.push(reason);
  };

  if (counts.x > 0) add(6, 'x_link', 'signal_has_x_link');
  if (counts.github > 0) add(5, 'github_link', 'signal_has_github_link');
  if (counts.website > 0) add(4, 'website_link', 'signal_has_project_website');
  if (counts.telegram > 0) add(3, 'telegram_link', 'signal_has_telegram');
  if (counts.discord > 0) add(2, 'discord_link', 'signal_has_discord');
  if (links.length === 0) add(-3, 'no_links', 'signal_has_no_links');

  if (hasAny(narrativeLower, [
    /\bai\s+agent(s)?\b/,
    /\bagentic\b/,
    /\bautonomous\s+agent(s)?\b/,
    /\bartificial intelligence\b/,
    /\bdefai\b/,
    /\bxai\b/,
    /\bgrok\b/,
  ])) {
    add(5, 'ai_agent', 'ai_or_agent_theme');
  }
  if (hasAny(narrativeLower, [/\bgithub\b/, /\bopen\s*source\b/, /\bdeveloper\b/, /\bdevtool\b/, /\bterminal\b/, /\bcli\b/])) {
    add(4, 'dev_tool', 'developer_tool_theme');
  }
  if (hasAny(narrativeLower, [/\btrump\b/, /\bmusk\b/, /\belon\b/, /\bbinance\b/, /\bcz\b/, /\bvitalik\b/])) {
    add(3, 'attention_narrative', 'attention_or_personality_theme');
  }
  if (hasAny(narrativeLower, [/\bcto\b/, /community takeover/, /community\s+takeover/])) {
    add(3, 'cto', 'community_takeover_theme');
  }
  if (hasAny(narrativeLower, [/\bpump\.fun\b/, /\bletsbonk\b/, /\bmoonshot\b/, /\bbonding curve\b/])) {
    add(2, 'launchpad', 'launchpad_theme');
  }

  score = Math.max(0, Math.min(30, score));
  const confidence = Math.max(0, Math.min(95, 20 + links.length * 8 + tags.length * 6));

  return {
    source: 'signal_text_parser',
    score,
    confidence,
    tags,
    reasons,
    links,
    counts,
  };
}

export default {
  extractSignalLinks,
  scoreNarrativeFeatures,
};
