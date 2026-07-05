//! docsgen — turns `docs/*.md` into the branded marekvs documentation website.
//!
//! Pipeline (single pass, deterministic, no network):
//!   1. read `docs/_nav.toml` for site metadata + ordered sidebar sections
//!   2. for every page: split frontmatter, render Markdown → HTML, syntax-
//!      highlight code (syntect, build-time), lift admonition fences to
//!      callouts, collect an in-page table of contents
//!   3. wrap each page in the branded `page.html` shell; render the bespoke
//!      `landing.html`; copy the vendored brand assets
//!   4. write `_site/` (clean URLs) ready for GitHub Pages
//!
//! Output dir: `$DOCS_OUT` or `<repo>/_site`. Base path (for a project Pages
//! site served under a subpath) comes from `$DOCS_BASE` or `[site].base`.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use pulldown_cmark::{
    html, CodeBlockKind, Event, HeadingLevel, Options, Parser, Tag, TagEnd,
};
use serde::Deserialize;
use syntect::highlighting::ThemeSet;
use syntect::html::highlighted_html_for_string;
use syntect::parsing::SyntaxSet;

// ── config (docs/_nav.toml) ────────────────────────────────────────────────

#[derive(Deserialize)]
struct Nav {
    site: SiteMeta,
    #[serde(default, rename = "section")]
    sections: Vec<Section>,
}

#[derive(Deserialize)]
struct SiteMeta {
    title: String,
    tagline: String,
    github: String,
    #[serde(default)]
    base: String,
}

#[derive(Deserialize)]
struct Section {
    title: String,
    pages: Vec<String>,
}

// ── a rendered page ────────────────────────────────────────────────────────

struct Page {
    title: String,
    description: String,
    status: String, // "", "implemented", "mixed", "planned"
    body: String,
    toc: Vec<Toc>,
}

struct Toc {
    level: u8,
    id: String,
    text: String,
}

struct Ctx {
    ss: SyntaxSet,
    theme: syntect::highlighting::Theme,
}

fn main() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo = manifest.parent().and_then(Path::parent).unwrap().to_path_buf();
    let theme_dir = manifest.join("theme");
    let docs_dir = repo.join("docs");
    let out = std::env::var("DOCS_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo.join("_site"));

    let nav: Nav = toml::from_str(
        &fs::read_to_string(docs_dir.join("_nav.toml")).expect("read docs/_nav.toml"),
    )
    .expect("parse _nav.toml");

    let base = std::env::var("DOCS_BASE").unwrap_or_else(|_| nav.site.base.clone());
    let base = base.trim_end_matches('/').to_string();

    let ss = SyntaxSet::load_defaults_newlines();
    let ts = ThemeSet::load_defaults();
    let theme = ts.themes["base16-ocean.dark"].clone();
    let ctx = Ctx { ss, theme };

    // 1. render every page referenced by the nav.
    let mut pages: HashMap<String, Page> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for section in &nav.sections {
        for slug in &section.pages {
            if pages.contains_key(slug) {
                continue;
            }
            let md = fs::read_to_string(docs_dir.join(format!("{slug}.md")))
                .unwrap_or_else(|e| panic!("read docs/{slug}.md: {e}"));
            pages.insert(slug.clone(), render_page(slug, &md, &ctx));
            order.push(slug.clone());
        }
    }

    // 2. build the shared left sidebar (needs every page's title).
    let sidebar_for = |active: &str| -> String {
        let mut s = String::new();
        for section in &nav.sections {
            s.push_str(&format!(
                "<div class=\"nav-section\"><span class=\"nav-section-title\">{}</span><ul>",
                esc(&section.title)
            ));
            for slug in &section.pages {
                let Some(p) = pages.get(slug) else { continue };
                let cls = if slug == active { " class=\"active\"" } else { "" };
                s.push_str(&format!(
                    "<li><a{cls} href=\"{base}/docs/{slug}/\">{}</a></li>",
                    esc(&p.title)
                ));
            }
            s.push_str("</ul></div>");
        }
        s
    };

    // 3. read templates once.
    let page_tpl = fs::read_to_string(theme_dir.join("templates/page.html")).expect("page.html");
    let landing_tpl =
        fs::read_to_string(theme_dir.join("templates/landing.html")).expect("landing.html");

    // 4. emit pages.
    for slug in &order {
        let p = &pages[slug];
        let toc_html = render_toc(&p.toc);
        let status = status_badge(&p.status);
        let html = fill(
            &page_tpl,
            &[
                ("base", &base),
                ("title", &format!("{} · {}", p.title, nav.site.title)),
                ("page_title", &p.title),
                ("description", &p.description),
                ("status", &status),
                ("sidebar", &sidebar_for(slug)),
                ("toc", &toc_html),
                ("content", &p.body),
                ("github", &nav.site.github),
                ("site_title", &nav.site.title),
            ],
        );
        let dir = out.join("docs").join(slug);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("index.html"), html).unwrap();
    }

    // 5. landing page.
    let landing = fill(
        &landing_tpl,
        &[
            ("base", &base),
            ("title", &format!("{} — {}", nav.site.title, nav.site.tagline)),
            ("tagline", &nav.site.tagline),
            ("github", &nav.site.github),
            ("site_title", &nav.site.title),
        ],
    );
    fs::create_dir_all(&out).unwrap();
    fs::write(out.join("index.html"), landing).unwrap();

    // 6. static assets + housekeeping.
    let assets = out.join("assets");
    copy_dir(&theme_dir.join("brand"), &assets.join("brand"));
    fs::create_dir_all(&assets).unwrap();
    fs::copy(theme_dir.join("site.css"), assets.join("site.css")).unwrap();
    fs::copy(theme_dir.join("site.js"), assets.join("site.js")).unwrap();
    fs::write(out.join(".nojekyll"), "").unwrap();
    fs::write(
        out.join("404.html"),
        fill(
            &page_tpl,
            &[
                ("base", &base),
                ("title", &format!("Not found · {}", nav.site.title)),
                ("page_title", &"Page not found".to_string()),
                ("description", &"That page doesn't exist.".to_string()),
                ("status", &String::new()),
                ("sidebar", &sidebar_for("")),
                ("toc", &String::new()),
                (
                    "content",
                    &format!(
                        "<p>That page doesn't exist. Head back to the \
                         <a href=\"{base}/\">home page</a> or the \
                         <a href=\"{base}/docs/overview/\">documentation</a>.</p>"
                    ),
                ),
                ("github", &nav.site.github),
                ("site_title", &nav.site.title),
            ],
        ),
    )
    .unwrap();

    println!(
        "docsgen: wrote {} pages + landing → {}",
        order.len(),
        out.display()
    );
}

// ── page rendering ─────────────────────────────────────────────────────────

fn render_page(slug: &str, md: &str, ctx: &Ctx) -> Page {
    let (front, body_md) = split_frontmatter(md);
    let title = front
        .get("title")
        .cloned()
        .unwrap_or_else(|| slug.replace('-', " "));
    let description = front.get("description").cloned().unwrap_or_default();
    let status = front.get("status").cloned().unwrap_or_default();
    let mut toc = Vec::new();
    let body = render_markdown(body_md, ctx, &mut toc);
    Page {
        title,
        description,
        status,
        body,
        toc,
    }
}

/// Render Markdown → HTML, collecting an h2/h3 table of contents, highlighting
/// code fences, and lifting admonition fences (```note / ```warning / …) to
/// styled callouts.
fn render_markdown(md: &str, ctx: &Ctx, toc: &mut Vec<Toc>) -> String {
    let mut opts = Options::empty();
    opts.insert(Options::ENABLE_TABLES);
    opts.insert(Options::ENABLE_FOOTNOTES);
    opts.insert(Options::ENABLE_STRIKETHROUGH);
    opts.insert(Options::ENABLE_TASKLISTS);

    let events: Vec<Event> = Parser::new_ext(md, opts).collect();
    let mut out: Vec<Event> = Vec::with_capacity(events.len());
    let mut seen: HashMap<String, u32> = HashMap::new();

    let mut i = 0;
    while i < events.len() {
        match &events[i] {
            Event::Start(Tag::Heading { level, .. }) => {
                let end = find_end(&events, i, |e| matches!(e, TagEnd::Heading(_)));
                let inner = &events[i + 1..end];
                let inner_html = render_events(inner);
                let text = plain_text(inner);
                let mut id = slugify(&text);
                let n = seen.entry(id.clone()).or_insert(0);
                if *n > 0 {
                    id = format!("{id}-{n}");
                }
                *seen.get_mut(&slugify(&text)).unwrap() += 1;
                let lvl = level_num(*level);
                if lvl == 2 || lvl == 3 {
                    toc.push(Toc {
                        level: lvl,
                        id: id.clone(),
                        text: text.clone(),
                    });
                }
                out.push(Event::Html(
                    format!(
                        "<h{lvl} id=\"{id}\"><a class=\"anchor\" href=\"#{id}\" \
                         aria-label=\"permalink\">#</a>{inner_html}</h{lvl}>\n"
                    )
                    .into(),
                ));
                i = end + 1;
            }
            Event::Start(Tag::CodeBlock(kind)) => {
                let lang = match kind {
                    CodeBlockKind::Fenced(s) => s.to_string(),
                    CodeBlockKind::Indented => String::new(),
                };
                let end = find_end(&events, i, |e| matches!(e, TagEnd::CodeBlock));
                let mut code = String::new();
                for e in &events[i + 1..end] {
                    if let Event::Text(t) = e {
                        code.push_str(t);
                    }
                }
                let mut toks = lang.split_whitespace();
                let kw = toks.next().unwrap_or("").to_ascii_lowercase();
                let rest: String = toks.collect::<Vec<_>>().join(" ");
                if let Some(cls) = admonition_class(&kw) {
                    // Accept both `​```note My title` and `​```note title="My title"`.
                    let title = if rest.is_empty() {
                        admonition_title(&kw)
                    } else if let Some(t) = rest.strip_prefix("title=") {
                        t.trim_matches('"').trim_matches('\'').to_string()
                    } else {
                        rest
                    };
                    let inner = render_markdown(code.trim_end(), ctx, &mut Vec::new());
                    out.push(Event::Html(
                        format!(
                            "<div class=\"callout {cls}\"><div class=\"callout-title\">{}</div>\
                             <div class=\"callout-body\">{inner}</div></div>\n",
                            esc(&title)
                        )
                        .into(),
                    ));
                } else {
                    out.push(Event::Html(highlight(&code, &kw, ctx).into()));
                }
                i = end + 1;
            }
            _ => {
                out.push(events[i].clone());
                i += 1;
            }
        }
    }

    let mut s = String::new();
    html::push_html(&mut s, out.into_iter());
    s
}

/// Render a slice of (inline) events to an HTML fragment.
fn render_events(events: &[Event]) -> String {
    let mut s = String::new();
    html::push_html(&mut s, events.iter().cloned());
    s
}

fn plain_text(events: &[Event]) -> String {
    let mut s = String::new();
    for e in events {
        match e {
            Event::Text(t) | Event::Code(t) => s.push_str(t),
            _ => {}
        }
    }
    s
}

fn find_end(events: &[Event], start: usize, is_end: impl Fn(&TagEnd) -> bool) -> usize {
    for (off, e) in events[start + 1..].iter().enumerate() {
        if let Event::End(te) = e {
            if is_end(te) {
                return start + 1 + off;
            }
        }
    }
    events.len() - 1
}

fn highlight(code: &str, lang: &str, ctx: &Ctx) -> String {
    let syntax = ctx
        .ss
        .find_syntax_by_token(lang)
        .unwrap_or_else(|| ctx.ss.find_syntax_plain_text());
    let raw = highlighted_html_for_string(code, &ctx.ss, syntax, &ctx.theme)
        .unwrap_or_else(|_| format!("<pre>{}</pre>", esc(code)));
    // syntect emits `<pre style="background-color:#…;">…</pre>`; swap the tag
    // for our own class so the brand ink background + chrome apply.
    let inner = raw
        .find('>')
        .map(|p| &raw[p + 1..])
        .unwrap_or(&raw)
        .trim_end()
        .strip_suffix("</pre>")
        .unwrap_or(&raw);
    let label = if lang.is_empty() { "text" } else { lang };
    format!(
        "<div class=\"code\"><span class=\"code-lang\">{}</span>\
         <button class=\"code-copy\" type=\"button\" aria-label=\"Copy\">copy</button>\
         <pre><code>{inner}</code></pre></div>\n",
        esc(label)
    )
}

fn render_toc(toc: &[Toc]) -> String {
    if toc.is_empty() {
        return String::new();
    }
    let mut s = String::from("<div class=\"toc-title\">On this page</div><ul>");
    for t in toc {
        let cls = if t.level == 3 { " class=\"sub\"" } else { "" };
        s.push_str(&format!(
            "<li{cls}><a href=\"#{}\">{}</a></li>",
            t.id,
            esc(&t.text)
        ));
    }
    s.push_str("</ul>");
    s
}

fn status_badge(status: &str) -> String {
    let (cls, label) = match status {
        "implemented" => ("ok", "Implemented"),
        "planned" => ("planned", "Planned"),
        "mixed" => ("mixed", "Implemented · some Planned"),
        _ => return String::new(),
    };
    format!("<span class=\"status status-{cls}\">{label}</span>")
}

// ── small helpers ──────────────────────────────────────────────────────────

fn split_frontmatter(src: &str) -> (HashMap<String, String>, &str) {
    let mut map = HashMap::new();
    let Some(rest) = src.strip_prefix("---\n") else {
        return (map, src);
    };
    let Some(end) = rest.find("\n---") else {
        return (map, src);
    };
    for line in rest[..end].lines() {
        if let Some((k, v)) = line.split_once(':') {
            let v = v.trim().trim_matches('"').trim_matches('\'');
            map.insert(k.trim().to_string(), v.to_string());
        }
    }
    let body = &rest[end + 4..];
    let body = body.strip_prefix('\n').unwrap_or(body);
    (map, body)
}

fn slugify(s: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = false;
    for c in s.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            prev_dash = false;
        } else if !prev_dash && !out.is_empty() {
            out.push('-');
            prev_dash = true;
        }
    }
    out.trim_matches('-').to_string()
}

fn level_num(l: HeadingLevel) -> u8 {
    match l {
        HeadingLevel::H1 => 1,
        HeadingLevel::H2 => 2,
        HeadingLevel::H3 => 3,
        HeadingLevel::H4 => 4,
        HeadingLevel::H5 => 5,
        HeadingLevel::H6 => 6,
    }
}

fn admonition_class(kw: &str) -> Option<&'static str> {
    match kw {
        "note" | "info" | "tip" => Some("info"),
        "warning" | "caution" => Some("warning"),
        "success" | "done" => Some("success"),
        "danger" | "error" => Some("error"),
        "planned" => Some("planned"),
        _ => None,
    }
}

fn admonition_title(kw: &str) -> String {
    let s = match kw {
        "note" => "Note",
        "info" => "Info",
        "tip" => "Tip",
        "warning" => "Warning",
        "caution" => "Caution",
        "success" | "done" => "Done",
        "danger" | "error" => "Careful",
        "planned" => "Planned",
        _ => "Note",
    };
    s.to_string()
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn fill(tpl: &str, vars: &[(&str, &String)]) -> String {
    let mut s = tpl.to_string();
    for (k, v) in vars {
        s = s.replace(&format!("{{{{{k}}}}}"), v);
    }
    s
}

fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            fs::copy(&path, to.join(entry.file_name())).unwrap();
        }
    }
}
