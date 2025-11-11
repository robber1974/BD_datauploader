# BD_Data_Uploader_v0_95.py
# Streamlit app to upload/update BlueDolphin objects + create relationships
# This software is under an MIT License (see root of project)
# v0.95:
#    - Help update: Added link to https://bluedolphin-key-buddy.lovable.app/ for easy user api key creation
#    - Help update: some additional changes for more clarity.
# v0.94:
#   - Add option to store and re-use configuration settings.
#   - Fixed matching by ID: now case insensitive; both for objects and relations.
#   - Added support for Dutch lifecycle input.
#   - Skip test existing when Label selected is (none).
#   - Shows empty label text instead of "nan" value when excel cell is empty.
# v0.93:
#   - Relationships preview: keep the FIRST (from_id,to_id[,label]) and mark all later
#     duplicates as "Skip: duplicate". First one still checks/obeys "Skip (exists)" and
#     "Skip: missing object".
#   - "Create" rows render with black text.
# v0.92:
#   - Added black text for Create rows; preview allowed but skipped all duplicates
# v0.91:
#   - Objects: optional Lifecycle, validation & preview coloring

import json, time, sys, subprocess, datetime, re
from email.utils import parsedate_to_datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Dict, List, Tuple, Optional, Iterable

import pandas as pd
import requests
import streamlit as st

# Accepted input values (either English or Dutch). For API payload we use the canonical English values.
ACCEPTABLE_LIFECYCLE_INPUT = {"Current", "Future", "Huidig", "Toekomst"}
ALLOWED_LIFECYCLE = {"Current", "Future"}  # canonical values used when sending to API

def _canonical_lifecycle(val: str) -> Optional[str]:
    """Return canonical English lifecycle ('Current'|'Future') for an input value, or None if unknown/empty."""
    if val is None: return None
    v = str(val).strip()
    if not v: return None
    m = v.lower()
    if m == "current": return "Current"
    if m == "future": return "Future"
    if m == "huidig": return "Current"
    if m == "toekomst": return "Future"
    return None

def _lifecycle_lang(val: str) -> Optional[str]:
    """Return 'en' or 'nl' depending on language of the provided value; None for empty/unknown."""
    if val is None: return None
    v = str(val).strip()
    if not v: return None
    m = v.lower()
    if m in ("current", "future"): return "en"
    if m in ("huidig", "toekomst"): return "nl"
    return None

st.set_page_config(page_title="BlueDolphin Uploader v0.94", layout="wide")
st.title("BlueDolphin CSV/Excel Uploader")

# ---------------- Sidebar: connection + mode ----------------
with st.sidebar:
    st.header("Connection")
    region = st.selectbox("Region", ["EU", "US"], index=0)
    API_BASE = "https://public-api.eu.bluedolphin.app/v1" if region == "EU" else "https://public-api.us.bluedolphin.app/v1"
    tenant = st.text_input("Tenant", placeholder="mytenant")
    api_key = st.text_input("x-api-key", type="password")

    st.divider()
    mode = st.radio("Mode", ["Objects", "Relationships"], index=0, horizontal=True)

    st.divider()

# ---------------- Configuration (sidebar): Reset + Save + Load ----------------
with st.sidebar.expander("Configuration", expanded=False):
    # Reset the current configuration
    if st.button("Reset configuration (keep tenant & key)"):
        keep = {"debug_mode","log_show_ok"}
        for k in list(st.session_state.keys()):
            if k in keep:
                continue
            if re.match(r"^(prop_bd_|prop_csv_|boem_bd_|boem_csv_)", k):
                st.session_state.pop(k, None)
        for k in ("prop_row_count","boem_row_count","preview_df","preview_mask_change","preview_mask_invalid",
                  "preview_meta","multi_value_sep","rel_prev","rel_ctx",
                  "obj_csvsep_choice","obj_csvsep_custom","obj_vsep_choice","obj_vsep_custom",
                  "map_title","map_id","map_lifecycle","confirm_apply_invalid"):
            st.session_state.pop(k, None)
        st.session_state["obj_upload_session"] = st.session_state.get("obj_upload_session", 0) + 1
        st.session_state["rel_upload_session"] = st.session_state.get("rel_upload_session", 0) + 1
        st.rerun()

    # Save configuration: create a local JSON file and offer to download it client-side
    if st.button("Save configuration (without key)"):
        # Build a snapshot of session_state excluding secrets and ephemeral UI/server objects
        exclude_prefixes = ("preview_")
        exclude_keys = {
            "api_key"
        }

        cfg = {}
        for k, v in st.session_state.items():
            if k in exclude_keys:
                continue
            if any(k.startswith(p) for p in exclude_prefixes):
                continue
            # Only include simple serializable types (fallback to str for others)
            try:
                json.dumps(v)
                cfg[k] = v
            except Exception:
                cfg[k] = str(v)

        cfg_json = json.dumps(cfg, indent=2, ensure_ascii=False)

        # Provide the JSON as a downloadable file (client-side download, no server save)
        date_str = datetime.date.today().strftime("%Y-%m-%d")
        download_name = f"bdu_config_{date_str}.json"
        st.download_button(
            label="Download configuration JSON",
            data=cfg_json,
            file_name=download_name,
            mime="application/json"
        )

    # Load configuration: select a local JSON file and apply into st.session_state
    cfg_file = st.file_uploader("Load configuration JSON (without key)", type=["json"], key="cfg_loader")
    if cfg_file:
        try:
            raw = cfg_file.read()
            try:
                text = raw.decode("utf-8")
            except Exception:
                text = raw.decode("latin-1")
            cfg = json.loads(text)
            if not isinstance(cfg, dict):
                st.error("Configuration file must contain a JSON object (key/value map)."); cfg = None
        except Exception as e:
            st.error(f"Failed to read configuration file: {e}"); cfg = None

        if cfg is not None:
            include_secrets = st.checkbox("Also restore API key (sensitive)", value=False, key="cfg_include_secrets")
            if st.button("Apply loaded configuration", key="cfg_apply_btn"):
                # Keys/prefixes we never restore by default
                exclude_prefixes = ("preview_", "obj_uploader", "rel_uploader")
                exclude_keys = {
                    "log_entries", "log_placeholder", "cfg_loader", "rate_box",
                    "cfg_apply_btn", "cfg_include_secrets",
                    "obj_apply_btn", "rel_apply_btn", "obj_preview_btn", "rel_preview_btn",
                    "obj_upload_session", "rel_upload_session"
                }

                applied = 0
                skipped = 0
                for k, v in cfg.items():
                    if k in ("tenant", "api_key") and not include_secrets:
                        skipped += 1; continue
                    if k in exclude_keys:
                        skipped += 1; continue
                    if any(k.startswith(p) for p in exclude_prefixes):
                        skipped += 1; continue
                    # Apply value (simple assignment)
                    st.session_state[k] = v
                    applied += 1

                st.success(f"Applied {applied} keys, skipped {skipped} keys.")

                # Clear any preview-state that might have been present in the loaded config so the
                # app waits for the user to press "Generate preview" before enabling "Apply now".
                for _k in ("preview_df", "preview_mask_change", "preview_mask_invalid", "preview_meta",
                           "confirm_apply_invalid"):
                    st.session_state.pop(_k, None)
                # Ensure UI returns to the mapping step (step 4) after rerun and shows the Generate preview button.
                st.session_state["obj_current_step"] = 4
                # st.rerun()

# --- state init ---
for k, v in [
    ("log_entries", []),
    ("log_placeholder", None),
    ("rate_box", None),
    ("debug_mode", False),
    ("log_show_ok", False),
    ("obj_upload_session", 0),
    ("rel_upload_session", 0),
    ("prop_row_count", 1),
    ("boem_row_count", 1),
]:
    if k not in st.session_state: st.session_state[k] = v

# ---------- Help (sidebar) ----------
def render_help():
    with st.sidebar.expander("Help: API key, tenant & rules", expanded=False):
        st.markdown(
            """
**API key & tenant**
- Create or use a **User API key** (inherits your permissions). Treat it like a password. Use either the link below or e.g. postman to create this.

        )
        st.link_button(
            "BlueDolphin User Key Buddy",
            "https://bluedolphin-key-buddy.lovable.app/",
        )

- The **Quick Start Guide** gives all the information about how to create the user API key.

        )
        st.link_button(
            "Open Quick Start Guide",
            "https://support.valueblue.nl/hc/en-us/articles/13296899552668-Quick-Start-Guide",
        )
The quick summary of the quick start
    - Key Management API Key = the key created in BlueDolphin of type "user key management" to create user API keys.
    - The tenantname = the part after **bluedolphin.app/**. Example: https://bluedolphin.app/mytenant → tenant = **mytenant**.
    - Userid = the Id of the user you want to create an API key for. Select the user in admin\\users and copy the Id from the url.
    - Key Name = The name you want to give the give. Only used for recognition (e.g. streamlit key for userX)
    - Expiration Date = The date the user API key expires. Treat this key as a password! Change it regularly!

**Objects — how it works**
- **Title** is required and must be **unique in your file**.
- **Object ID** (optional). If present, we update by ID; otherwise we match by Title.
- **Lifecycle** (optional): **Current** or **Future**. Empty = ignored; invalid values are flagged.
- **Questionnaires**
  - **Dropdown/Radio**: use one of the listed options.
  - **Multiselect**: set your file’s separator in the UI; we convert to `|` automatically.
  - **Checkbox**: only **Yes** or **No** (case-sensitive).
  - **Numbers/Currency**: up to **16 digits**; decimals `.` or `,`; scientific notation (like `1.2E+26`) is **not allowed**.
- **Preview colours**
  - 🟩 **green** = no change
  - 🔵 **blue** = will change
  - 🟥 **red** = invalid (will be sent as empty/undefined if you proceed)
- **Large uploads**: if the preview shows **>100** objects, applying can take longer.
- **Rate limits**: if we need to slow down, the app shows a countdown and retries automatically.

**Relationships — how it works**
- **Relationship template ID**: enter the ID manually (the API doesn’t list them).
- **From / To**: pick the definitions and map IDs or Titles.
- **Existing check**: we **skip** creating a relation if one already exists with the same **from**, **to**, **relationship type**, **direction**, and **label**.
- **Label**: optional free text attached to the relation.
- **Lifecycle** (optional): Current or Future.
- After apply, links to the **Relations** tab of each “from” object are shown.
"""
        )
        st.link_button(
            "Open Quick Start Guide",
            "https://support.valueblue.nl/hc/en-us/articles/13296899552668-Quick-Start-Guide",
        )

render_help()

# ---------------- Advanced (sidebar): logging + cache ----------------
with st.sidebar.expander("Advanced", expanded=False):
    st.session_state.debug_mode = st.checkbox("Enable logging", value=st.session_state.debug_mode)
    st.session_state.log_show_ok = st.checkbox("Include successes (2xx) & pre-calls", value=st.session_state.log_show_ok)
    cA, cB = st.columns(2)
    with cA:
        if st.button("Clear log"):
            st.session_state.log_entries = []
    with cB:
        if st.button("Reload data (clear cache)"):
            st.cache_data.clear()
            for k in ("preview_df","preview_mask_change","preview_mask_invalid","preview_meta","rel_prev","rel_ctx","confirm_apply_invalid"):
                st.session_state.pop(k, None)

    if st.session_state["log_placeholder"] is None:
        st.session_state["log_placeholder"] = st.empty()
    def _render_log():
        show_ok = st.session_state.log_show_ok
        filtered = []
        for entry in reversed(st.session_state.log_entries):
            if entry["level"] in ("ok", "info") and not show_ok:
                continue
            filtered.append(entry)
        filtered = filtered[:20]
        html_lines = []
        for e in filtered:
            color = "#B00020" if e["level"] == "error" else ("#666" if e["level"] == "info" else "#1f4b2e")
            html_lines.append(
                f'<div style="font-family: ui-monospace, Menlo, Consolas, monospace; white-space: pre;"><span style="color:{color}">{e["text"]}</span></div>'
            )
        html = (
            '<div style="border:1px solid #ddd; border-radius:6px; padding:8px; height:22em; overflow:auto; background:#fafafa;">'
            + "".join(html_lines) + '</div>'
        )
        st.session_state["log_placeholder"].markdown(html, unsafe_allow_html=True)
    _render_log()

def _log(level: str, text: str):
    st.session_state.log_entries.append({"level": level, "text": text})

def _is_logging() -> bool:
    return bool(st.session_state.debug_mode)

# ---------------- Connection cache scoping ----------------
def hdr() -> Dict[str, str]:
    return {"tenant": tenant or "", "x-api-key": api_key or "", "Content-Type": "application/json"}

conn_key = f"{region}|{tenant}|{(api_key or '')[:8]}"
if st.session_state.get("conn_key") != conn_key:
    st.cache_data.clear()
    st.session_state.conn_key = conn_key
    for k in ("preview_df","preview_mask_change","preview_mask_invalid","preview_meta","rel_prev","rel_ctx","confirm_apply_invalid"):
        st.session_state.pop(k, None)

if st.session_state.rate_box is None:
    st.session_state.rate_box = st.empty()

# ---------------- Request helper with retry + countdown ----------------
def _retry_after_seconds(resp, fallback: int) -> int:
    h = resp.headers.get("Retry-After")
    if not h: return fallback
    try:
        return max(1, int(h))
    except Exception:
        try:
            dt = parsedate_to_datetime(h)
            if not dt.tzinfo: dt = dt.replace(tzinfo=datetime.timezone.utc)
            wait = int((dt - datetime.datetime.now(datetime.timezone.utc)).total_seconds())
            return max(1, wait)
        except Exception:
            return fallback

def _request(method: str, path: str, *, params: Dict=None, body: Dict=None,
             expect_json: bool=True, log: bool=True, ui_errors: bool=True,
             ui_feedback: bool=True, retry_on_429: bool=True,
             wait_seconds: int=20, max_retries: int=4, respect_retry_after: bool=True):
    url = f"{API_BASE}{path}"

    if _is_logging() and log and st.session_state.log_show_ok:
        body_keys = list(body.keys()) if isinstance(body, dict) else None
        prop_cnt = len(body.get("object_properties", [])) if isinstance(body, dict) and "object_properties" in body else 0
        boem_cnt = sum(len(q.get("items", [])) for q in body.get("boem", [])) if isinstance(body, dict) and "boem" in body else 0
        _log("info", f">>> {method} {path} {params or ''}"
                    f"{(' body_keys=' + str(body_keys)) if body_keys else ''}"
                    f"{(' props=' + str(prop_cnt)) if prop_cnt else ''}"
                    f"{(' boem_items=' + str(boem_cnt)) if boem_cnt else ''}")

    attempt = 0
    wait_box_side = st.session_state.rate_box if ui_feedback else None
    wait_box_main = st.empty() if ui_feedback else None

    while True:
        attempt += 1
        r = requests.request(
            method, url,
            headers=hdr(),
            params=params or {},
            data=(json.dumps(body) if body is not None else None),
            timeout=60
        )
        status_line = f"[{r.status_code}] {method} {path}"

        if r.status_code < 400:
            if _is_logging() and log: _log("ok", status_line)
            if wait_box_side: wait_box_side.empty()
            if wait_box_main: wait_box_main.empty()
            if expect_json and r.text:
                try: return r.json()
                except Exception: return r.text
            return r.text or ""

        if r.status_code == 429 and retry_on_429 and attempt <= max_retries:
            wait_for = _retry_after_seconds(r, wait_seconds) if respect_retry_after else wait_seconds
            if _is_logging() and log:
                _log("info", f"{status_line} waiting {wait_for}s (attempt {attempt}/{max_retries})")
            if ui_feedback:
                for i in range(wait_for, 0, -1):
                    msg = f"Working… waiting {i}s before retry (attempt {attempt}/{max_retries})"
                    if wait_box_side: wait_box_side.warning(msg)
                    if wait_box_main: wait_box_main.info(msg)
                    time.sleep(1)
                if wait_box_main: wait_box_main.empty()
            continue

        msg = (r.text or "").strip()
        if r.status_code == 429 and retry_on_429 and attempt > max_retries:
            msg = f"Gave up after {max_retries} retries. {msg}"
        if ui_errors:
            st.error(f"{status_line}: {msg[:800]}")
        if _is_logging() and log:
            _log("error", f"{status_line} {msg[:800]}")
        raise requests.HTTPError(f"{status_line}: {msg}")

def get_json(path: str, params: Dict=None, **kw):  return _request("GET", path, params=params, expect_json=True, **kw)
def post_json(path: str, body: Dict, **kw):        return _request("POST", path, body=body, expect_json=True, **kw)
def patch_json(path: str, body: Dict, **kw):       return _request("PATCH", path, body=body, expect_json=True, **kw)

# ---------------- Cached fetchers ----------------
@st.cache_data(show_spinner=False)
def list_workspaces_cached(api_base: str, tenant_: str, api_key_: str):
    return get_json("/workspaces", log=False, ui_errors=False, ui_feedback=False)

@st.cache_data(show_spinner=False)
def list_object_definitions_cached(api_base: str, tenant_: str, api_key_: str):
    data = get_json("/object-definitions", log=False, ui_errors=False, ui_feedback=False)
    return data.get("items", data)

# ---------------- API wrappers ----------------
def list_objects(workspace_id: str, object_def_id: str, take: int = 2000):
    data = get_json("/objects", params={"workspace_id": workspace_id, "filter": object_def_id, "take": take})
    return data.get("items", data) or []
def get_object(obj_id: str):              return get_json(f"/objects/{obj_id}")
def get_object_definition(def_id: str):   return get_json(f"/object-definitions/{def_id}")
def get_questionnaire(q_id: str):         return get_json(f"/questionnaires/{q_id}")
def create_object(title: str, object_def_id: str, workspace_id: str, lifecycle: Optional[str] = None):
    body = {"object_title": title, "object_type_id": object_def_id, "workspace_id": workspace_id}
    if lifecycle in ALLOWED_LIFECYCLE:
        body["object_lifecycle_state"] = lifecycle
    return post_json("/objects", body)
def patch_object(obj_id: str, body: Dict): return patch_json(f"/objects/{obj_id}", body)

# -------- Relationships API --------
def get_relation(rel_id: str) -> dict:
    return get_json(f"/relations/{rel_id}")

def post_relationship(template_id: str, from_id: str, to_id: str, label: Optional[str], lifecycle: Optional[str]):
    body = {
        "template_id": template_id,
        "from_object_id": from_id,
        "to_object_id": to_id,
    }
    if label:
        body["label"] = str(label)
    if lifecycle in {"Current", "Future"}:
        body["relationship_lifecycle_state"] = lifecycle
    return post_json("/relations", body)

# ===== Utilities for object properties by NAME =====
def _iter_dict_lists(d: Dict) -> Iterable[List[Dict]]:
    for k, v in (d or {}).items():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            yield v

def _extract_property_names_from_definition(defn: Dict) -> List[str]:
    names: List[str] = []
    if isinstance(defn.get("object_properties"), list):
        for p in defn["object_properties"]:
            name = p.get("name")
            if name: names.append(str(name))
    if names: return sorted(dict.fromkeys(names))
    seen = []
    for lst in _iter_dict_lists(defn):
        for p in lst:
            nm = p.get("name")
            if nm: seen.append(str(nm))
    return sorted(dict.fromkeys(seen))

def _discover_property_names_from_objects(workspace_id: str, object_def_id: str, sample: int = 10) -> List[str]:
    names: List[str] = []
    try:
        objs = list_objects(workspace_id, object_def_id, take=max(50, sample))
        for o in objs[:sample]:
            try:
                full = get_object(o["id"])
                for it in full.get("object_properties", []):
                    nm = it.get("name")
                    if nm: names.append(str(nm))
            except Exception:
                pass
    except Exception:
        pass
    return sorted(dict.fromkeys(names))

# ====== Mapping-row auto-add callbacks (Objects) ======
def _maybe_add_prop_row(last_idx: int):
    bd = st.session_state.get(f"prop_bd_{last_idx}", "(select)")
    csvv = st.session_state.get(f"prop_csv_{last_idx}", "(select)")
    if last_idx == st.session_state.get("prop_row_count", 1) - 1:
        if bd and bd != "(select)" and csvv and csvv != "(select)":
            st.session_state["prop_row_count"] = st.session_state.get("prop_row_count", 1) + 1

def _maybe_add_boem_row(last_idx: int):
    bd = st.session_state.get(f"boem_bd_{last_idx}", "(select)")
    csvv = st.session_state.get(f"boem_csv_{last_idx}", "(select)")
    if last_idx == st.session_state.get("boem_row_count", 1) - 1:
        if bd and bd != "(select)" and csvv and csvv != "(select)":
            st.session_state["boem_row_count"] = st.session_state.get("boem_row_count", 1) + 1

# ====== Value normalization & comparison (Objects) ======
def _classify_type(ft: str) -> str:
    ft = (ft or "").lower()
    if ft == "checkbox": return "checkbox"
    if ft in {"multiselect"} or "multi" in ft: return "dropdown_multi"
    if ft in {"dropdown", "radio", "select", "combobox"}: return "dropdown_single"
    if ft in {"currency"}: return "currency"
    if ft in {"number", "numeric", "decimal", "float", "integer"}: return "number"
    if ft in {"date", "datetime"}: return "date"
    return "text"

def _parse_decimal_like(s: str) -> Optional[Decimal]:
    if s is None: return None
    v = str(s).strip()
    if v == "": return None
    if "e" in v.lower(): return None
    v = v.replace(" ", "")
    dot = v.rfind("."); com = v.rfind(",")
    if dot != -1 and com != -1:
        if dot > com:
            v = v.replace(",", "")
        else:
            v = v.replace(".", "")
            v = v.replace(",", ".")
    else:
        if "," in v and "." not in v:
            v = v.replace(",", ".")
    try:
        return Decimal(v)
    except InvalidOperation:
        return None

def _quantize(d: Decimal, decimals: Optional[int]) -> Decimal:
    if decimals is None: return d
    q = Decimal(10) ** -decimals
    return d.quantize(q, rounding=ROUND_HALF_UP)

def _canon_for_compare(cfg: Dict, raw_val: str, multi_value_sep: str) -> str:
    typ = cfg.get("type","text")
    if typ == "dropdown_multi":
        if raw_val is None: return ""
        parts = [p.strip() for p in str(raw_val).split(multi_value_sep) if str(p).strip()!=""]
        return "|".join(sorted(parts))
    if typ in {"number","currency"}:
        d = _parse_decimal_like(raw_val)
        if d is None: return ""
        d = _quantize(d, cfg.get("decimals"))
        return f"{d:f}"
    return "" if raw_val is None else str(raw_val).strip()

def _canon_for_payload(cfg: Dict, raw_val: str, multi_value_sep: str) -> str:
    typ = cfg.get("type","text")
    if raw_val is None: return ""
    if typ == "dropdown_multi":
        parts = [p.strip() for p in str(raw_val).split(multi_value_sep) if str(p).strip()!=""]
        return "|".join(parts)
    if typ in {"number","currency"}:
        d = _parse_decimal_like(raw_val)
        if d is None: return ""
        d = _quantize(d, cfg.get("decimals"))
        decs = cfg.get("decimals")
        if decs is not None:
            fmt = f"{{0:.{decs}f}}"
            return fmt.format(d).replace(",", ".")
        return f"{d:f}"
    return str(raw_val).strip()

# ========= OBJECTS FLOW =========
def objects_flow():
    if not tenant or not api_key:
        st.info("Enter **Tenant** and **API key** in the sidebar to start.")
        return

    st.header("1) Pick workspace & object definition")
    try:
        ws = list_workspaces_cached(API_BASE, tenant, api_key); ws_map = {w["name"]: w["id"] for w in ws}
    except Exception as e:
        st.error(e); st.stop()
    workspace = st.selectbox("Workspace", sorted(ws_map.keys()), key="obj_ws"); workspace_id = ws_map[workspace]

    try:
        obj_defs = list_object_definitions_cached(API_BASE, tenant, api_key); od_map = {od.get("name", od.get("id")): od["id"] for od in obj_defs}
    except Exception as e:
        st.error(e); st.stop()
    objdef_label = st.selectbox("Object definition", sorted(od_map.keys()), key="obj_def"); object_def_id = od_map[objdef_label]

    # ---------------- Step 2 ----------------
    def ensure_pkg(pkg: str) -> bool:
        try:
            __import__(pkg); return True
        except ImportError:
            try:
                with st.spinner(f"Installing {pkg}…"):
                    subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
                __import__(pkg); return True
            except Exception as e:
                st.error(f"Failed to install {pkg}: {e}"); return False

    st.header("2) Upload CSV / Excel")
    up = st.file_uploader("Choose file", type=["csv", "xlsx", "xls", "xlsm"], key=f"obj_uploader_{st.session_state.obj_upload_session}")
    if not up: st.stop()

    is_csv = up.name.lower().endswith(".csv")
    csv_col_sep = None

    if is_csv:
        with st.expander("CSV options", expanded=False):
            sep_choice = st.selectbox(
                "CSV column separator",
                ["Auto-detect", "Comma (,)", "Semicolon (;)", "Pipe (|)", "Tab (\\t)", "Custom…"],
                index=0, key="obj_csvsep_choice"
            )
            if sep_choice == "Auto-detect": csv_col_sep = None
            elif sep_choice == "Comma (,)": csv_col_sep = ","
            elif sep_choice == "Semicolon (;)": csv_col_sep = ";"
            elif sep_choice == "Pipe (|)": csv_col_sep = "|"
            elif sep_choice == "Tab (\\t)": csv_col_sep = "\t"
            else:
                csv_col_sep = st.text_input("Custom separator (1–3 chars)", value=";", max_chars=3, key="obj_csvsep_custom") or ";"

    try:
        n = up.name.lower()
        if n.endswith(".csv"):
            if csv_col_sep is None: df = pd.read_csv(up)
            else: df = pd.read_csv(up, sep=csv_col_sep, engine="python")
        elif n.endswith((".xlsx", ".xlsm")):
            if not ensure_pkg("openpyxl"): st.stop()
            df = pd.read_excel(up, engine="openpyxl")
        else:
            if not ensure_pkg("xlrd"): st.stop()
            df = pd.read_excel(up, engine="xlrd")
    except Exception as e:
        st.error(f"Could not read file: {e}"); st.stop()
    if df.empty:
        st.error("The uploaded file is empty."); st.stop()
    df.columns = [str(c) for c in df.columns]

    # ---------------- Step 3 (mapping) ----------------
    st.header("3) Mapping")

    with st.spinner("Loading questionnaires & properties…"):
        definition = get_object_definition(object_def_id)

        related_boem = definition.get("related_boem") or []
        questionnaires = []
        for q in related_boem:
            try:
                questionnaires.append(get_questionnaire(q["id"]))
            except Exception:
                pass

        prop_names = _extract_property_names_from_definition(definition)
        if not prop_names:
            prop_names = _discover_property_names_from_objects(workspace_id, object_def_id, sample=10)
        if _is_logging():
            _log("info", f"object_properties discovered (by name): {len(prop_names)}")

    boem_field_options: Dict[str, Tuple[str, str, str, str]] = {}
    field_config: Dict[Tuple[str, str], Dict] = {}
    has_multi = False

    for q in questionnaires:
        qname = q.get("name", q.get("id"))
        for f in q.get("fields", []):
            ftype_raw = f.get("field_type") or f.get("type") or ""
            ftype = _classify_type(ftype_raw)
            if (ftype_raw or "").lower() == "relation":
                continue
            fid = f.get("id"); fname = f.get("name"); qid = q.get("id")
            label = f"{qname} – {fname}"
            boem_field_options[label] = (qid, fid, qname, fname)

            allowed = set()
            if ftype in {"dropdown_single", "dropdown_multi"}:
                for opt in f.get("specified_values", []) or []:
                    if isinstance(opt, dict):
                        val = opt.get("Value") or opt.get("value") or opt.get("name") or opt.get("label")
                    else:
                        val = str(opt)
                    if val is not None: allowed.add(str(val))

            if ftype == "dropdown_multi": has_multi = True

            field_config[(qid, fid)] = {
                "type": ftype,
                "allowed": allowed,
                "decimals": f.get("number_of_decimals", None) if ftype in {"number","currency"} else None
            }

    if has_multi:
        with st.expander("Multi-select value separator", expanded=True):
            vsep_choice = st.selectbox(
                "Separator used in your file for multi-select questionnaire values",
                ["|", ";", ",", "/", "Custom…"],
                index=0, key="obj_vsep_choice"
            )
            if vsep_choice == "Custom…":
                multi_value_sep = st.text_input("Custom multi-select separator (1–3 chars)", value="|", max_chars=3, key="obj_vsep_custom") or "|"
            else:
                multi_value_sep = vsep_choice
        st.caption("We validate with this separator and **normalize to `|`** in the API payload.")
    else:
        multi_value_sep = "|"

    # Title + ID + Lifecycle mapping
    c1, c2 = st.columns([1, 1])
    with c1: st.markdown("**Object Title (required)**")
    with c2: title_col = st.selectbox("CSV column for title", list(df.columns), key="map_title", label_visibility="collapsed")

    c3, c4 = st.columns([1, 1])
    with c3: st.markdown("Object ID (optional)")
    with c4: object_id_col = st.selectbox("CSV column for object_id", ["(none)"] + list(df.columns), key="map_id", label_visibility="collapsed")

    c5, c6 = st.columns([1, 1])
    with c5: st.markdown("Lifecycle (optional)")
    with c6: lifecycle_col = st.selectbox("CSV column for lifecycle", ["(none)"] + list(df.columns), key="map_lifecycle", label_visibility="collapsed")

    # Object properties (by NAME)
    st.divider(); st.subheader("Object properties (optional)")
    for i in range(st.session_state.prop_row_count):
        current = st.session_state.get(f"prop_bd_{i}", "(select)")
        picked = set()
        for j in range(st.session_state.prop_row_count):
            if j >= i: continue
            v = st.session_state.get(f"prop_bd_{j}", "(select)")
            if v and v != "(select)":
                picked.add(v)
        prop_opts = ["(select)"] + [p for p in prop_names if (p not in picked) or (p == current)]
        cA, cB = st.columns([1, 1])
        with cA:
            st.selectbox("Property (name)", prop_opts, key=f"prop_bd_{i}",
                         on_change=_maybe_add_prop_row, args=(i,))
        with cB:
            st.selectbox("CSV column", ["(select)"] + list(df.columns), key=f"prop_csv_{i}",
                         on_change=_maybe_add_prop_row, args=(i,))
    prop_map: Dict[str, str] = {}
    for i in range(st.session_state.prop_row_count):
        bd = st.session_state.get(f"prop_bd_{i}", "(select)")
        csvc = st.session_state.get(f"prop_csv_{i}", "(select)")
        if bd != "(select)" and csvc != "(select)": prop_map[bd] = csvc

    # Questionnaires
    st.divider(); st.subheader("Questionnaires (optional)")
    boem_labels_all = list(boem_field_options.keys())
    for i in range(st.session_state.boem_row_count):
        current = st.session_state.get(f"boem_bd_{i}", "(select)")
        picked = set()
        for j in range(st.session_state.boem_row_count):
            if j >= i: continue
            v = st.session_state.get(f"boem_bd_{j}", "(select)")
            if v and v != "(select)":
                picked.add(v)
        boem_opts = ["(select)"] + [lbl for lbl in boem_labels_all if (lbl not in picked) or (lbl == current)]
        cA, cB = st.columns([1, 1])
        with cA:
            st.selectbox("Questionnaire – Field", boem_opts, key=f"boem_bd_{i}",
                         on_change=_maybe_add_boem_row, args=(i,))
        with cB:
            st.selectbox("CSV column", ["(select)"] + list(df.columns), key=f"boem_csv_{i}",
                         on_change=_maybe_add_boem_row, args=(i,))
    boem_map: Dict[Tuple[str,str,str,str], str] = {}
    for i in range(st.session_state.boem_row_count):
        bd = st.session_state.get(f"boem_bd_{i}", "(select)")
        csvc = st.session_state.get(f"boem_csv_{i}", "(select)")
        if bd != "(select)" and csvc != "(select)":
            boem_map[boem_field_options[bd]] = csvc

    # Title uniqueness
    if not title_col:
        st.error("Select a CSV column for **Object Title**."); st.stop()
    titles_series = df[title_col].astype(str).str.strip()
    dupes = titles_series[titles_series.duplicated(keep=False) & titles_series.ne("")]
    if not dupes.empty:
        sample = dupes.unique()[:10]
        st.error(f"**Object Title** must be unique. Found {dupes.nunique()} duplicates. Examples: {', '.join(map(str, sample))}")
        st.stop()

    # ---------------- Step 4 (preview) ----------------
    st.header("4) Preview")
    # If a loaded configuration requested to return to mapping, show a short notice
    if st.session_state.get("obj_current_step") == 4:
        st.info("Configuration applied. Review mapping and press **Generate preview** to build the preview.")
        # Clear the flag so the notice only shows once after rerun
        st.session_state.pop("obj_current_step", None)

    preview_clicked = st.button("Generate preview", key="obj_preview_btn")
    if preview_clicked:
        with st.spinner("Retrieving existing objects…"):
            existing = list_objects(workspace_id, object_def_id)

        by_id = {o["id"]: o for o in existing if "id" in o}
        by_title = {str((o.get("object_title") or o.get("title"))): o for o in existing if (o.get("object_title") or o.get("title"))}
        # Build a lower-case lookup to allow case-insensitive matching of IDs later
        lower_by_id = {str(k).lower(): k for k in by_id.keys()}

        def get_detail(stub):
            try: return get_object(stub["id"])
            except Exception: return {"id": stub["id"], "object_title": stub.get("object_title") or stub.get("title") or "", "object_properties": [], "boem": []}

        def read_boem(detail):
            out={}
            for q in detail.get("boem", []):
                qid=q.get("id"); 
                if not qid: continue
                out[qid]={it["id"]:str(it.get("value","")) for it in q.get("items", [])}
            return out

        def read_props_by_name(detail):
            curr={}
            for it in detail.get("object_properties", []):
                pname = it.get("name")
                if pname: curr[str(pname)] = str(it.get("value",""))
            return curr

        preview_cols = ["Action","Object_Title","Id","Lifecycle"]
        prop_cols = [f"objectproperty_{name}" for name in prop_map.keys()]
        preview_cols += prop_cols
        boem_cols = [f"questionnaire({qname})_{fname}" for (_,_,qname,fname) in boem_map.keys()]
        preview_cols += boem_cols

        rows, change_rows, invalid_rows, meta = [], [], [], []

        def _digits_len_ok(val: str, max_digits: int = 16) -> bool:
            digits = re.findall(r"\d", val or "")
            return 0 < len(digits) <= max_digits
        def _only_numberish_chars(val: str) -> bool:
            return re.fullmatch(r"\s*-?[\d.,\s]+\s*", val or "") is not None
        def _validate_value(qid: str, fid: str, raw_val: str) -> bool:
            cfg = field_config.get((qid, fid), {"type":"text","allowed":set()})
            t = cfg["type"]; allowed = cfg.get("allowed", set())
            v = "" if raw_val is None else str(raw_val).strip()
            if v == "": return True
            if t == "checkbox": return v in {"Yes", "No"}
            if t in {"radio", "dropdown_single"}:
                return (len(allowed)==0) or (v in allowed)
            if t == "dropdown_multi":
                parts = [p.strip() for p in str(v).split(multi_value_sep) if p.strip()!=""]
                if len(allowed)==0: return True
                return all(p in allowed for p in parts)
            if t in {"number", "currency"}:
                if "e" in v.lower(): return False
                if not _only_numberish_chars(v): return False
                return _digits_len_ok(v, 16) and (_parse_decimal_like(v) is not None)
            return True
        def _equivalent(qid: str, fid: str, newv: str, oldv: str) -> bool:
            cfg = field_config.get((qid, fid), {"type":"text"})
            c_new = _canon_for_compare(cfg, newv, multi_value_sep)
            c_old = _canon_for_compare(cfg, oldv, "|")
            return c_new == c_old

        # ---- v0.93 duplicate handling: keep first, skip later ----
        seen_keys = set()

        for _, r in df.iterrows():
            title_target = str(r.get(title_col, "")).strip()
            if not title_target: continue
            obj_id_val = "" if object_id_col == "(none)" else str(r.get(object_id_col, "")).strip()
            life_raw = "" if lifecycle_col == "(none)" else (str(r.get(lifecycle_col, "") or "").strip())
            # Derive canonical English value (for comparison / payload) and detected language
            life_canon = _canonical_lifecycle(life_raw)
            life_lang = _lifecycle_lang(life_raw)

            target_props: Dict[str, str] = {pname: ("" if pd.isna(r.get(csvc,"")) else str(r.get(csvc,"")))
                                            for pname, csvc in prop_map.items()}
            target_boem: Dict[str, Dict[str, str]] = {}
            for (qid,fid,qname,fname), csvc in boem_map.items():
                val = "" if pd.isna(r.get(csvc,"")) else str(r.get(csvc,""))
                target_boem.setdefault(qid, {})[fid] = val

            # Resolve by ID if provided; if ID not found, attempt to fall back to Title.
            # Track unresolved/mismatched ID so we can mark it in the preview.
            had_id = bool(obj_id_val)
            # case-insensitive ID lookup
            id_found = False
            if had_id:
                id_found = str(obj_id_val).lower() in lower_by_id
            title_found = (title_target in by_title) and (by_title.get(title_target) is not None)

            id_unresolved = False
            stub = None
            if had_id:
                if id_found:
                    # resolve real key case-insensitively
                    real_key = lower_by_id.get(str(obj_id_val).lower())
                    stub = by_id.get(real_key)
                else:
                    # ID was provided but not found — try title fallback
                    if title_found:
                        stub = by_title.get(title_target)
                        # mark that the provided ID did not resolve but we used the title match
                        id_unresolved = True
                    else:
                        # ID provided but not found and title not found => no match
                        stub = None
                        id_unresolved = True
            else:
                stub = by_title.get(title_target)

            # Build duplicate key only when both IDs are known (after resolving)
            curr_id = ""
            if stub:
                curr_id = stub["id"]
            key_for_dupes = None  # (from_id,to_id[,label]) is only for relationships (below)

            if stub:
                # --- Update path (same as before) ---
                detail = get_detail(stub)
                curr_title = str(detail.get("object_title",""))
                curr_boem = read_boem(detail)
                curr_props = read_props_by_name(detail)
                curr_life = str(detail.get("object_lifecycle_state","") or "")

                row = {"Action":"Update","Object_Title":title_target,"Id":detail["id"],"Lifecycle":life_raw}
                # mark Id invalid if the CSV supplied an ID that didn't resolve
                mask_change = {"Action":False,"Object_Title":(title_target!=curr_title),"Id":False,"Lifecycle":False}
                mask_invalid = {"Action":False,"Object_Title":False,"Id":bool(id_unresolved),"Lifecycle":False}
                any_change = (title_target!=curr_title)

                if life_raw != "":
                    # Accept both Dutch and English inputs; determine canonical English value.
                    if life_canon is None:
                        mask_invalid["Lifecycle"] = True
                    else:
                        chg_life = (life_canon != curr_life)
                        mask_change["Lifecycle"] = chg_life
                        any_change |= chg_life

                for pname in prop_map.keys():
                    key = f"objectproperty_{pname}"
                    newv = target_props.get(pname, "")
                    oldv = curr_props.get(pname, "")
                    row[key]=newv
                    changed = (str(newv)!=str(oldv))
                    mask_change[key]=changed
                    mask_invalid[key]=False
                    any_change |= changed

                for (qid,fid,qname,fname) in {(q,f,qn,fn) for (q,f,qn,fn) in boem_map.keys()}:
                    key=f"questionnaire({qname})_{fname}"
                    newv = target_boem.get(qid, {}).get(fid, "")
                    oldv = curr_boem.get(qid, {}).get(fid, "")
                    row[key]=newv
                    valid = _validate_value(qid,fid,newv)
                    changed = (not _equivalent(qid,fid,newv,oldv))
                    mask_change[key]=changed
                    mask_invalid[key]= (changed and not valid)
                    any_change |= changed

                if any_change:
                    rows.append(row); change_rows.append(mask_change); invalid_rows.append(mask_invalid)
                    boem_updates: Dict[str, Dict[str,str]] = {}
                    for (qid,fid,qn,fn), csvc in boem_map.items():
                        newv = target_boem.get(qid, {}).get(fid, "")
                        oldv = curr_boem.get(qid, {}).get(fid, "")
                        if not _equivalent(qid,fid,newv,oldv):
                            boem_updates.setdefault(qid, {})[fid] = newv
                    prop_updates = {pname: target_props[pname]
                                    for pname in prop_map.keys()
                                    if str(target_props[pname]) != str(curr_props.get(pname, ""))}
                    lifecycle_update = life_canon if (life_canon is not None and life_canon != curr_life) else None
                    meta.append({
                        "new": False, "id": detail["id"],
                        "original_id_provided": obj_id_val if had_id else None,
                        "id_mismatch": bool(id_unresolved),
                        "title_update": title_target if (title_target!=curr_title) else None,
                        "title": title_target,
                        "boem_updates": boem_updates,
                        "prop_updates": prop_updates,
                        "lifecycle_update": lifecycle_update,
                        "lifecycle_raw": (life_raw or None),
                        "lifecycle_lang": life_lang
                    })
            else:
                # --- Create path (same as before) ---
                row={"Action":"Create","Object_Title":title_target,"Id":"","Lifecycle":life_raw}
                # If the CSV supplied an ID that couldn't be found, mark Id invalid so user can see mismatch.
                mask_change={"Action":False,"Object_Title":True,"Id":False,"Lifecycle": (life_raw != "" and life_raw in ALLOWED_LIFECYCLE)}
                mask_invalid={"Action":False,"Object_Title":False,"Id":bool(id_unresolved),"Lifecycle": (life_raw != "" and life_raw not in ALLOWED_LIFECYCLE)}
                for pname in prop_map.keys():
                    key=f"objectproperty_{pname}"
                    v = target_props.get(pname, "")
                    row[key]=v; mask_change[key]=True; mask_invalid[key]=False
                for (qid,fid,qname,fname) in boem_map.keys():
                    key=f"questionnaire({qname})_{fname}"
                    v = target_boem.get(qid, {}).get(fid, "")
                    row[key]=v; mask_change[key]=True
                    mask_invalid[key]= (v!="" and not _validate_value(qid,fid,v))
                rows.append(row); change_rows.append(mask_change); invalid_rows.append(mask_invalid)
                lifecycle_val = life_canon if life_canon is not None else None
                meta.append({
                    "new": True, "id": "", "title": title_target, "boem": target_boem, "props": target_props,
                    "lifecycle": lifecycle_val,
                    "original_id_provided": obj_id_val if had_id else None,
                    "id_mismatch": bool(id_unresolved),
                    "lifecycle_raw": (life_raw or None),
                    "lifecycle_lang": life_lang
                })

        if not rows:
            st.info("Nothing to create or update based on current mapping.")
        else:
            preview_df = pd.DataFrame(rows, columns=preview_cols)
            change_df = pd.DataFrame(False, index=preview_df.index, columns=preview_df.columns)
            invalid_df = pd.DataFrame(False, index=preview_df.index, columns=preview_df.columns)
            for i, (mc, mi) in enumerate(zip(change_rows, invalid_rows)):
                for k, v in mc.items():
                    if k in change_df.columns: change_df.loc[preview_df.index[i], k] = bool(v)
                for k, v in mi.items():
                    if k in invalid_df.columns: invalid_df.loc[preview_df.index[i], k] = bool(v)

            RED_BG, RED_FG   = "#ff8a8a", "#6b0000"
            BLUE_BG, BLUE_FG = "#cfe8ff", "#084298"
            GREEN_BG, GREEN_FG = "#b6f3b6", "#064b2d"

            def style_cell(val, col, idx):
                if col in ("Action"): return ""
                # Only suppress styling for the Id column when the user did NOT map an Object ID column.
                if col == "Id":
                    if object_id_col == "(none)":
                        return ""
                    # For Id column: only show invalid (red). Don't show green/blue for OK/changed.
                    inv_id = bool(invalid_df.loc[idx, col]) if col in invalid_df.columns else False
                    if inv_id:
                        bg, fg = RED_BG, RED_FG
                        return f"background-color:{bg}; color:{fg}; font-weight:600;"
                    return ""
                inv = bool(invalid_df.loc[idx, col]) if col in invalid_df.columns else False
                chg = bool(change_df.loc[idx, col]) if col in change_df.columns else False
                if inv:   bg, fg = RED_BG, RED_FG
                elif chg: bg, fg = BLUE_BG, BLUE_FG
                else:     bg, fg = GREEN_BG, GREEN_FG
                return f"background-color:{bg}; color:{fg}; font-weight:600;"

            styled = preview_df.style.apply(lambda s: [style_cell(v, s.name, s.index[i]) for i, v in enumerate(s)], axis=0)
            st.dataframe(styled, use_container_width=True)

            st.session_state.preview_df = preview_df
            st.session_state.preview_mask_change = change_df
            st.session_state.preview_mask_invalid = invalid_df
            st.session_state.preview_meta = meta
            st.session_state.multi_value_sep = multi_value_sep

            invalid_count_preview = int(invalid_df.values.sum())
            st.success(f"Preview ready: {len(preview_df)} rows • Invalid field values detected: {invalid_count_preview}")

    # ---------------- Step 5 (apply) ----------------
    st.header("5) Apply changes")

    invalid_count = 0
    if "preview_mask_invalid" in st.session_state and st.session_state["preview_mask_invalid"] is not None:
        invalid_count = int(st.session_state["preview_mask_invalid"].values.sum())

    confirm_ok = True
    if invalid_count > 0:
        st.warning(
            f"Some questionnaire/lifecycle values look misconfigured ({invalid_count} cell(s)). "
            "Applying may result in those fields becoming **empty/undefined** in BlueDolphin."
        )
        confirm_ok = st.checkbox(
            "I understand and want to proceed",
            key="confirm_apply_invalid",
            value=st.session_state.get("confirm_apply_invalid", False),
        )

    apply_disabled = ("preview_df" not in st.session_state) or (invalid_count > 0 and not confirm_ok)

    if st.button("Apply now", disabled=apply_disabled, key="obj_apply_btn"):
        if "preview_df" not in st.session_state:
            st.warning("Click **Generate preview** first."); st.stop()

        meta = st.session_state.preview_meta
        created = updated = errors = 0
        created_list: List[Tuple[str, str]] = []
        updated_list: List[Tuple[str, str]] = []
        logs = []
        prog = st.progress(0.0, text="Working…")
        multi_value_sep = st.session_state.get("multi_value_sep","|")

        def boem_payload_from_dict(d: Dict[str, Dict[str,str]]):
            payload = []
            for qid, fields in d.items():
                items = []
                for fid, val in fields.items():
                    cfg = field_config.get((qid, fid), {"type":"text"})
                    items.append({"id": fid, "value": _canon_for_payload(cfg, val, multi_value_sep)})
                if items: payload.append({"id": qid, "items": items})
            return payload

        def props_payload_from_dict_namekey(d: Dict[str, str]):
            return [{"name": pname, "value": v} for pname, v in d.items()]

        def _obj_url(obj_id: str) -> str:
            return f"https://bluedolphin.app/{tenant}/objects/all/item/{obj_id}"

        for i, item in enumerate(meta):
            try:
                if item["new"]:
                    res = create_object(item["title"], object_def_id, workspace_id, lifecycle=item.get("lifecycle"))
                    new_id = res.get("id")
                    patch_body = {}
                    if item.get("props"):
                        patch_body["object_properties"] = props_payload_from_dict_namekey(item["props"])
                    if item.get("boem"):
                        patch_body["boem"] = boem_payload_from_dict(item["boem"])
                    if patch_body:
                        patch_object(new_id, patch_body)
                    created += 1
                    created_list.append((item["title"], new_id))
                    logs.append(f"Create → \"{item['title']}\" ({new_id})")
                else:
                    patch_body = {}
                    if item.get("title_update"):
                        patch_body["object_title"] = item["title_update"]
                    prop_clean = {pname: v for pname, v in (item.get("prop_updates") or {}).items()}
                    if prop_clean:
                        patch_body["object_properties"] = props_payload_from_dict_namekey(prop_clean)
                    boem_clean_src = (item.get("boem_updates") or {})
                    if boem_clean_src:
                        patch_body["boem"] = boem_payload_from_dict(boem_clean_src)
                    life_upd = item.get("lifecycle_update")
                    if life_upd in ALLOWED_LIFECYCLE:
                        patch_body["object_lifecycle_state"] = life_upd
                    if patch_body:
                        patch_object(item["id"], patch_body)
                        updated += 1
                        updated_list.append((item.get("title") or "", item["id"]))
                        logs.append(f"Update → \"{item.get('title') or ''}\" ({item['id']})")
            except Exception as e:
                errors += 1; logs.append(f"ERROR: {e}")
            prog.progress((i+1)/max(1,len(meta)))

        st.success(f"Done — Created: {created} • Updated: {updated} • Errors: {errors}")

        if created_list:
            st.subheader("Created")
            st.markdown("\n".join([f"- **{t}** → [{obj_id}]({_obj_url(obj_id)})" for t, obj_id in created_list]))
        if updated_list:
            st.subheader("Updated")
            st.markdown("\n".join([f"- **{t}** → [{obj_id}]({_obj_url(obj_id)})" for t, obj_id in updated_list]))

        st.code("\n".join(logs), language="text")

    count = st.session_state.get("preview_df").shape[0] if "preview_df" in st.session_state else 0
    msg = f"Preview shows target values; **green = unchanged**, **blue = valid change**, **red = invalid**. Objects in preview: {count}."
    if count > 100: msg += " This may take a little while to complete."
    st.caption(msg)

# ========= RELATIONSHIPS FLOW =========
_obj_rel_cache: Dict[str, dict] = {}
_rel_detail_cache: Dict[str, dict] = {}

def _get_object_cached(obj_id: str) -> dict:
    if obj_id not in _obj_rel_cache:
        _obj_rel_cache[obj_id] = get_object(obj_id)
    return _obj_rel_cache[obj_id]

def _get_relation_cached(rel_id: str) -> dict:
    if rel_id not in _rel_detail_cache:
        _rel_detail_cache[rel_id] = get_relation(rel_id)
    return _rel_detail_cache[rel_id]

def relation_exists_exact(template_id: str, from_id: str, to_id: str, label: Optional[str]) -> bool:
    """
    Check whether a relation with given template/from/to exists.
    If label is None, LABEL is ignored (match only template/from/to).
    If label is provided (including empty string), the label must match exactly.
    Comparison of IDs is done case-insensitively.
    """
    if not (template_id and from_id and to_id):
        return False

    # normalize for comparisons
    tpl_norm = str(template_id).strip()
    from_norm = str(from_id).strip()
    to_norm = str(to_id).strip()
    tpl_norm_l = tpl_norm.lower()
    from_norm_l = from_norm.lower()
    to_norm_l = to_norm.lower()

    label_is_none = (label is None)
    label_norm = "" if label is None else str(label)

    try:
        # Try to fetch the source object as provided (may fail if casing mismatch)
        src = _get_object_cached(from_id)
    except Exception:
        # If fetching by provided id fails, try a best-effort: attempt to find an object
        # in the local cache whose id matches case-insensitively (if available).
        # If none found, give up and return False.
        found = None
        for obj in _obj_rel_cache.values():
            try:
                oid = str(obj.get("id", "")).strip()
                if oid and oid.lower() == from_norm_l:
                    found = obj
                    break
            except Exception:
                continue
        if not found:
            return False
        src = found

    rels = src.get("related_objects", []) or []
    for r in rels:
        try:
            obj_id = str(r.get("object_id") or "").strip()
            if not obj_id or obj_id.lower() != to_norm_l:
                continue

            tpl = str(
                (r.get("relationship") or {}).get("template_id")
                or (r.get("type") or {}).get("id")
                or ""
            ).strip()
            if tpl.lower() != tpl_norm_l:
                continue

            rid = r.get("relationship_id")
            if not rid:
                continue
            det = _get_relation_cached(str(rid))

            src_id = str(det.get("source_id") or "").strip()
            tgt_id = str(det.get("target_id") or "").strip()
            if src_id.lower() != from_norm_l:
                continue
            if tgt_id.lower() != to_norm_l:
                continue

            # If caller passed label=None -> ignore label and consider this a match.
            if label_is_none:
                return True

            existing_label = str(det.get("remark") or det.get("label") or "")
            if existing_label == label_norm:
                return True
        except Exception:
            continue
    return False

def relationships_flow():
    if not tenant or not api_key:
        st.info("Enter **Tenant** and **API key** in the sidebar to start.")
        return

    st.header("R1) Pick workspace & definitions")

    try:
        ws = list_workspaces_cached(API_BASE, tenant, api_key); ws_map = {w["name"]: w["id"] for w in ws}
    except Exception as e:
        st.error(e); st.stop()
    workspace = st.selectbox("Workspace", sorted(ws_map.keys()), key="rel_ws"); workspace_id = ws_map[workspace]

    try:
        obj_defs = list_object_definitions_cached(API_BASE, tenant, api_key)
        def_map = {od.get("name", od.get("id")): od["id"] for od in obj_defs}
    except Exception as e:
        st.error(e); st.stop()

    col_from, col_to = st.columns(2)
    with col_from:
        from_name = st.selectbox("From: Object definition", sorted(def_map.keys()), key="rel_from_def")
        from_def_id = def_map[from_name]
    with col_to:
        to_name = st.selectbox("To: Object definition", sorted(def_map.keys()), key="rel_to_def")
        to_def_id = def_map[to_name]

    st.info("The API does not expose relationship definitions. Enter the **Relationship Template ID** manually.")

    # Allow user to choose source: manual entry or JSON config (with dropdown).
    src = st.radio("Template ID source", ["Manual entry", "Use JSON config (dropdown)"], index=0, horizontal=True, key="rel_tpl_source")

    def _parse_rel_tpl_config(cfg):
        mapping = {}
        if isinstance(cfg, dict):
            for k, v in cfg.items():
                if isinstance(v, str):
                    mapping[str(k)] = v
                elif isinstance(v, dict):
                    vid = v.get("id") or v.get("template_id") or v.get("value") or v.get("templateId")
                    if vid:
                        mapping[str(k)] = vid
        elif isinstance(cfg, list):
            for item in cfg:
                if not isinstance(item, dict):
                    continue
                name = item.get("name") or item.get("label") or item.get("title") or item.get("template_name")
                vid = item.get("id") or item.get("template_id") or item.get("value") or item.get("templateId")
                if name and vid:
                    mapping[str(name)] = vid
        return mapping

    tpl_id = st.session_state.get("rel_tpl_manual", "") or ""

    if src == "Manual entry":
        # Manual input only
        # If the user previously selected a template in the JSON dropdown, make sure that selected
        # template ID is reflected in the manual input when switching back to Manual entry.
        prev_sel = st.session_state.get("rel_tpl_select", "(none)")
        rel_map = st.session_state.get("rel_tpl_map", {}) or {}
        if prev_sel and prev_sel != "(none)":
            # ensure the canonical manual session key contains the selected template id
            st.session_state["rel_tpl_manual"] = rel_map.get(prev_sel, st.session_state.get("rel_tpl_manual", ""))

        tpl_id = st.text_input("Relationship Template ID", key="rel_tpl_manual", placeholder="e.g. 5a00543aec9d264840ae0619")
        # keep any previously loaded config but do not show dropdown
        if st.session_state.get("rel_tpl_config") and st.button("Preview loaded templates", key="rel_tpl_preview_btn"):
            cfg_preview = st.session_state.get("rel_tpl_config")
            try:
                def _tpl_preview_rows(cfg):
                    rows = []
                    if isinstance(cfg, dict):
                        for k, v in cfg.items():
                            if isinstance(v, (str, int, float)):
                                rows.append({"name": str(k), "template_id": str(v)})
                            elif isinstance(v, dict):
                                vid = v.get("id") or v.get("template_id") or v.get("value") or v.get("templateId")
                                rows.append({
                                    "name": str(k),
                                    "template_id": str(vid) if vid is not None else "",
                                    "raw": json.dumps(v, ensure_ascii=False)
                                })
                            else:
                                rows.append({"name": str(k), "template_id": str(v)})
                    elif isinstance(cfg, list):
                        for item in cfg:
                            if not isinstance(item, dict):
                                rows.append({"name": "", "template_id": str(item)})
                                continue
                            name = item.get("name") or item.get("label") or item.get("title") or item.get("template_name") or ""
                            vid = item.get("id") or item.get("template_id") or item.get("value") or item.get("templateId") or ""
                            rows.append({
                                "name": str(name),
                                "template_id": str(vid),
                                "raw": json.dumps(item, ensure_ascii=False)
                            })
                    else:
                        rows.append({"name": "", "template_id": str(cfg)})
                    return rows

                rows = _tpl_preview_rows(cfg_preview)
                if rows:
                    df_preview = pd.DataFrame(rows)
                    st.info(f"Templates in loaded config: **{len(df_preview)}**.")
                    st.dataframe(df_preview, use_container_width=True)
                    # with st.expander("Show raw loaded JSON", expanded=False):
                    #     st.json(cfg_preview)
                else:
                    st.info("No templates found in loaded config.")
            except Exception:
                st.info("Loaded config could not be parsed as templates.")
    else:
        # Use JSON config path: file uploader + dropdown
        c1, c2 = st.columns([3, 1])
        with c1:
            rel_tpl_file = st.file_uploader("Upload templates (JSON)", type=["json"], key="rel_tpl_config_loader")
            # allow previously loaded config to remain if user doesn't upload again
            if rel_tpl_file:
                try:
                    cfg = json.load(rel_tpl_file)
                    st.session_state["rel_tpl_config"] = cfg
                    st.success("Loaded relationship templates configuration")
                    if isinstance(cfg, dict):
                        st.caption(f"{len(cfg)} templates loaded")
                except Exception as e:
                    st.error(f"Invalid JSON: {e}")
        # Build dropdown from session-stored config (if present)
        cfg_loaded = st.session_state.get("rel_tpl_config")
        if cfg_loaded:
            tpl_map = _parse_rel_tpl_config(cfg_loaded) or {}
            # persist the map in session for use by callbacks / post-select logic
            st.session_state["rel_tpl_map"] = tpl_map
            if tpl_map:
                opts = ["(none)"] + sorted(tpl_map.keys())

                # Determine which option should be selected by default:
                # priority: previously stored rel_tpl_select (if still valid) ->
                #            current rel_tpl_manual matches a value in tpl_map ->
                #            fallback "(none)"
                prev_select = st.session_state.get("rel_tpl_select")
                default_key = "(none)"
                if prev_select in opts:
                    default_key = prev_select
                else:
                    current_manual = str(st.session_state.get("rel_tpl_manual", "") or "").strip()
                    if current_manual:
                        # find the label that maps to this id
                        for label, vid in tpl_map.items():
                            if str(vid).strip() == current_manual:
                                default_key = label
                                break

                # Show the selectbox. Use the stored key so Streamlit persists the selection.
                try:
                    sel_index = opts.index(default_key)
                except Exception:
                    sel_index = 0

                st.selectbox("Choose template", opts, index=sel_index, key="rel_tpl_select")
                # Immediately reflect the selection into the manual tpl input so downstream code
                # reads a single canonical session key ("rel_tpl_manual").
                sel = st.session_state.get("rel_tpl_select", "(none)")
                rel_map = st.session_state.get("rel_tpl_map", {}) or {}
                if sel and sel != "(none)":
                    st.session_state["rel_tpl_manual"] = rel_map.get(sel, "")
                else:
                    # If user explicitly chose "(none)", clear the manual entry
                    # but keep any previously typed manual input if the selection wasn't "(none)".
                    if sel == "(none)":
                        st.session_state["rel_tpl_manual"] = st.session_state.get("rel_tpl_manual", "")
                # set tpl_id from the canonical session key
                tpl_id = st.session_state.get("rel_tpl_manual", "") or ""
            else:
                st.caption("No templates found in uploaded JSON.")
                tpl_id = ""
        else:
            st.info("Upload a JSON file describing templates, or switch to Manual entry.")
            tpl_id = ""

    st.header("R2) Upload & map CSV / Excel")
    file = st.file_uploader("Choose file", type=["csv", "xlsx", "xls", "xlsm"], key=f"rel_uploader_{st.session_state.rel_upload_session}")
    if not file: st.stop()

    def ensure_pkg(pkg: str) -> bool:
        try:
            __import__(pkg); return True
        except ImportError:
            try:
                with st.spinner(f"Installing {pkg}…"):
                    subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
                __import__(pkg); return True
            except Exception as e:
                st.error(f"Failed to install {pkg}: {e}"); return False

    try:
        n = file.name.lower()
        if n.endswith(".csv"): df = pd.read_csv(file)
        elif n.endswith((".xlsx", ".xlsm")):
            if not ensure_pkg("openpyxl"): st.stop()
            df = pd.read_excel(file, engine="openpyxl")
        else:
            if not ensure_pkg("xlrd"): st.stop()
            df = pd.read_excel(file, engine="xlrd")
    except Exception as e:
        st.error(f"Could not read file: {e}"); st.stop()
    if df.empty:
        st.error("The uploaded file is empty."); st.stop()
    df.columns = [str(c) for c in df.columns]

    cols = ["(none)"] + list(df.columns)
    m1, m2 = st.columns(2)
    with m1:
        from_id_col    = st.selectbox("From: Object ID column (optional)", cols, index=0, key="rel_from_id_col")
        from_title_col = st.selectbox("From: Object Title column (optional)", cols, index=0, key="rel_from_title_col")
    with m2:
        to_id_col    = st.selectbox("To: Object ID column (optional)", cols, index=0, key="rel_to_id_col")
        to_title_col = st.selectbox("To: Object Title column (optional)", cols, index=0, key="rel_to_title_col")

    l1, l2 = st.columns(2)
    with l1:
        label_col = st.selectbox("Label (remark) column (optional)", cols, index=0, key="rel_label_col")
    with l2:
        lifecycle_col_rel = st.selectbox("Lifecycle column (optional)", cols, index=0, key="rel_lifecycle_col")

    st.caption("Lifecycle accepted values: **Current** / **Future**")

    st.header("R3) Preview")

    if st.button("Generate relationship preview", type="primary", key="rel_preview_btn"):
        cache_by_pair: Dict[Tuple[str, str], pd.DataFrame] = {}

        def fetch_objects_df(ws_id: str, def_id: str) -> pd.DataFrame:
            key = (ws_id, def_id)
            if key not in cache_by_pair:
                items = list_objects(ws_id, def_id, take=10000)
                df_ = pd.DataFrame(items)
                if "id" not in df_.columns: df_["id"] = None
                if "title" not in df_.columns:
                    df_["title"] = df_.get("object_title") if "object_title" in df_.columns else None
                cache_by_pair[key] = df_[["id", "title"]].copy()
            return cache_by_pair[key]

        resolved_rows = []
        for _, r in df.iterrows():
            f_id = "" if from_id_col == "(none)" else str(r.get(from_id_col) or "").strip()
            t_id = "" if to_id_col == "(none)" else str(r.get(to_id_col) or "").strip()
            f_title = "" if from_title_col == "(none)" else str(r.get(from_title_col) or "").strip()
            t_title = "" if to_title_col == "(none)" else str(r.get(to_title_col) or "").strip()
            life = "" if lifecycle_col_rel == "(none)" else str(r.get(lifecycle_col_rel) or "").strip()
            # Use pd.isna to avoid turning pandas NaN into the string "nan"
            if label_col == "(none)":
                lbl = ""
            else:
                val = r.get(label_col)
                lbl = "" if pd.isna(val) else str(val).strip()

            if not f_id and f_title:
                df_from = fetch_objects_df(workspace_id, from_def_id)
                match = df_from[df_from["title"] == f_title]
                if len(match) == 1: f_id = match.iloc[0]["id"]
            if not t_id and t_title:
                df_to = fetch_objects_df(workspace_id, to_def_id)
                match = df_to[df_to["title"] == t_title]
                if len(match) == 1: t_id = match.iloc[0]["id"]

            resolved_rows.append({
                "from_id": f_id, "from_title": f_title,
                "to_id": t_id, "to_title": t_title,
                "label": lbl, "lifecycle": life
            })

        # v0.93: mark only later duplicates
        seen_keys: set = set()

        rows = []
        for rr in resolved_rows:
            f_id = rr["from_id"]; t_id = rr["to_id"]; lbl = rr["label"]; life = rr["lifecycle"]
            errs = []
            if not tpl_id: errs.append("Template ID missing")
            missing_ft = (not f_id) or (not t_id)

            key = (f_id, t_id) if label_col == "(none)" else (f_id, t_id, lbl)
            is_dup_later = (not missing_ft) and (key in seen_keys)

            exists = False
            if not missing_ft and not errs and not is_dup_later:
                try:
                    # Determine label argument based on whether a Label column was mapped.
                    # If no label column is mapped, pass None so relation_exists_exact ignores the label.
                    label_arg = None if label_col == "(none)" else (lbl or "").strip()
                    exists = relation_exists_exact(
                        template_id=str(tpl_id).strip(),
                        from_id=str(f_id).strip(),
                        to_id=str(t_id).strip(),
                        label=label_arg
                    )
                except Exception:
                    exists = False

            if is_dup_later:
                action = "Skip: duplicate"
            elif missing_ft:
                action = "Skip: missing object"
            else:
                action = "Skip (exists)" if exists else "Create"

            # After deciding action, remember we've seen this key (so later same keys are dupes)
            if not missing_ft:
                seen_keys.add(key)

            rows.append({
                "Action": action,
                "From_Title": rr["from_title"],
                "From_ID": f_id,
                "To_Title": rr["to_title"],
                "To_ID": t_id,
                "Template_ID": tpl_id,
                "Label": lbl,
                "Lifecycle": life,
                "Error": "; ".join(errs)
            })

        rel_prev = pd.DataFrame(rows)

        def style_row(row):
            styles = []
            for col in rel_prev.columns:
                bad = False
                if col in ("From_ID", "To_ID", "Template_ID"):
                    bad = not str(row[col]).strip()
                if col == "Error" and str(row[col]).strip():
                    bad = True

                action_lower = str(row["Action"]).strip().lower()
                is_skip = action_lower.startswith("skip")
                is_create = action_lower == "create"

                if is_skip:
                    styles.append("background-color:#efefef; color:#333; font-weight:600;")
                else:
                    bg = "#a9f0a9" if not bad else "#ff9a9a"
                    if is_create:
                        styles.append(f"background-color:{bg}; color:#333;")
                    else:
                        styles.append(f"background-color:{bg};")
            return styles

        st.dataframe(rel_prev.style.apply(lambda r: style_row(r), axis=1), use_container_width=True)
        st.session_state["rel_prev"] = rel_prev
        st.success(f"Preview ready: {len(rel_prev)} relationships")

    # Apply
    st.header("R4) Apply relationships")
    if st.button("Apply relationships", key="rel_apply_btn", disabled=("rel_prev" not in st.session_state)):
        if "rel_prev" not in st.session_state:
            st.warning("Click **Generate relationship preview** first."); st.stop()
        rel_prev = st.session_state["rel_prev"]
        to_create = rel_prev[rel_prev["Action"] == "Create"]
        if to_create.empty:
            st.info("Nothing to create."); st.stop()

        created = 0; errors = 0
        created_from_ids: set = set()
        prog = st.progress(0.0, text="Creating…")

        for i, row in to_create.reset_index(drop=True).iterrows():
            try:
                _ = post_relationship(
                    template_id=str(row["Template_ID"]).strip(),
                    from_id=str(row["From_ID"]).strip(),
                    to_id=str(row["To_ID"]).strip(),
                    label=str(row["Label"]).strip() if str(row["Label"]).strip() else None,
                    lifecycle=(str(row["Lifecycle"]).strip() or None)
                )
                created += 1
                created_from_ids.add(str(row["From_ID"]).strip())
            except Exception as e:
                errors += 1
                if _is_logging():
                    _log("error", f"POST /relations failed: {e}")
            prog.progress((i+1)/max(1, len(to_create)))

        st.success(f"Done — Created: {created} • Errors: {errors} • Skipped were shown in preview")

        if created_from_ids:
            st.subheader("Quick links to Relations tab (FROM objects)")
            lines = []
            for oid in sorted(created_from_ids):
                url = f"https://bluedolphin.app/{tenant}/objects/all/item/{oid}/relations"
                lines.append(f"- [{oid}]({url})")
            st.markdown("\n".join(lines))

# ---------------- Main router ----------------
if not tenant or not api_key:
    st.info("Enter **Tenant** and **API key** in the sidebar to begin.")
else:
    if mode == "Objects":
        objects_flow()
    else:
        relationships_flow()



