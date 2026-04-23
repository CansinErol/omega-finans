import base64
import hmac
import inspect
import io
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Optional

import pandas as pd
import streamlit as st
from supabase import create_client

log = logging.getLogger("omega.panel")


def _normalize_supabase_url(url: str) -> str:
    """PostgREST cift /rest/v1 veya bosluk kaynakli PGRST125 onlenir."""
    u = str(url).strip().rstrip("/")
    for suf in ("/rest/v1", "/rest/v1/"):
        while u.endswith(suf):
            u = u[: -len(suf)].rstrip("/")
    return u


def _jwt_role_unverified(token: str) -> Optional[str]:
    parts = token.strip().split(".")
    if len(parts) != 3:
        return None
    payload_b64 = parts[1]
    pad = -len(payload_b64) % 4
    if pad:
        payload_b64 += "=" * pad
    try:
        raw = base64.urlsafe_b64decode(payload_b64.encode("ascii"))
        data = json.loads(raw.decode("utf-8"))
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    role = data.get("role")
    return str(role) if role is not None else None


st.set_page_config(page_title="Omega Finans v14", layout="wide")
st.markdown(
    """
<style>
/* Mobil: çentik / durum çubuğu / Streamlit üst çubuğu — başlıkların kesilmemesi */
@media (max-width: 768px) {
    .stApp {
        padding-top: env(safe-area-inset-top, 0px);
    }
    [data-testid="stSidebar"] > div:first-child {
        padding-top: calc(0.5rem + env(safe-area-inset-top, 0px)) !important;
    }
    section.main > div.block-container,
    section.stMain > div.block-container,
    section[data-testid="stMain"] > div.block-container {
        padding-top: calc(3.25rem + env(safe-area-inset-top, 12px)) !important;
        padding-left: max(0.65rem, env(safe-area-inset-left, 0px)) !important;
        padding-right: max(0.65rem, env(safe-area-inset-right, 0px)) !important;
        padding-bottom: max(2rem, env(safe-area-inset-bottom, 0px)) !important;
    }
    h1 {
        font-size: 1.35rem !important;
        line-height: 1.25 !important;
        padding-top: 0.15rem !important;
    }
    h2 {
        font-size: 1.15rem !important;
        line-height: 1.25 !important;
    }
}
</style>
""",
    unsafe_allow_html=True,
)

URL = _normalize_supabase_url(st.secrets["SUPABASE_URL"])
KEY = str(st.secrets["SUPABASE_KEY"]).strip()

if KEY.startswith("sb_secret_"):
    st.error(
        "SUPABASE_KEY olarak sb_secret_ anahtarı kullanılamaz; yalnızca publishable veya "
        "legacy anon (eyJ...) kullanın."
    )
    st.stop()

if KEY.startswith("sb_publishable_"):
    pass
else:
    _key_role = _jwt_role_unverified(KEY)
    if _key_role == "service_role":
        st.error(
            "SUPABASE_KEY olarak service_role anahtarı kullanılamaz; yalnızca anon (public) "
            "Supabase anahtarını secrets.toml veya Streamlit Secrets içinde kullanın."
        )
        st.stop()
    if _key_role is not None and _key_role != "anon":
        log.warning("SUPABASE_KEY JWT rolü beklenmedik: %s (çoğu kurulumda 'anon' olur)", _key_role)

supabase = create_client(URL, KEY)
DEFAULT_VIEW_LIMIT = 200
CACHE_DATA_TTL = 180
FINANS_TIPLERI = ("Alacaklar", "Genel Giderler", "Malzeme Giderleri", "İşçilik Giderleri")


def kayit_sanitize(rec):
    return {k: (None if pd.isna(v) else v) for k, v in rec.items()}


def format_tutar_tr(deger):
    return f"{float(deger):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def format_tutar_tl_plain(deger):
    """Hesap ekstresi gösterimi: ₺ öneki, binlik ayırıcı yok, virgülle iki ondalık (örn. ₺ 900000,00)."""
    if deger is None or (isinstance(deger, float) and pd.isna(deger)):
        return "₺ 0,00"
    n = float(deger)
    s = f"{abs(n):.2f}"
    tam, on = s.split(".")
    if n < 0:
        return f"₺ -{tam},{on}"
    return f"₺ {tam},{on}"


def hesap_ekstresi_ozet_stili(tablo: pd.DataFrame):
    """Hesap ekstresi özet tablosu: Toplam Maliyet kırmızı; K/Z satırları işarete göre yeşil/kırmızı."""
    tut_col = "Tutar (₺)"
    kal_col = "Kalem"

    def satir_stil(row: pd.Series):
        stil = [""] * len(row)
        k = str(row.get(kal_col, ""))
        try:
            v = float(row.get(tut_col, 0))
        except (TypeError, ValueError):
            v = 0.0
        j = list(row.index).index(tut_col)
        if k == "Toplam Maliyet":
            stil[j] = "color: #c62828; font-weight: 600;"
        elif k in ("Güncel Kar/Zarar", "KDV ile hesaplanan Kar/Zarar"):
            renk = "#2e7d32" if v >= 0 else "#c62828"
            stil[j] = f"color: {renk}; font-weight: 600;"
        return stil

    def tutar_hucre_goster(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return format_tutar_tl_plain(0.0)
        return format_tutar_tl_plain(v)

    return tablo.style.apply(satir_stil, axis=1).format(subset=[tut_col], formatter=tutar_hucre_goster)


def tablo_kolonlari(tablo_tipi):
    if tablo_tipi == "Genel Giderler":
        return ["tarih", "harcamayi_yapan", "harcama_adi", "tutar"]
    if tablo_tipi == "İşçilik Giderleri":
        return ["odeme_tarihi", "taseron_odemeleri", "aciklama", "tutar", "odenecek_tutar"]
    if tablo_tipi == "Malzeme Giderleri":
        return ["bolum", "tarih", "firma", "aciklama", "odeme", "kullanilan_malzeme", "marka", "temin_edilen_firma", "odenecek_tutar"]
    return ["tarih", "aciklama", "borc", "alacak", "bakiye"]


@st.cache_data(ttl=CACHE_DATA_TTL, show_spinner=False)
def verileri_yukle(proje_adi, tablo_tipi, limit=DEFAULT_VIEW_LIMIT, with_id=False):
    try:
        res = (
            supabase.table("finans_verileri")
            .select("*")
            .eq("proje_adi", proje_adi)
            .eq("tip", tablo_tipi)
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        if res.data:
            df = pd.DataFrame(res.data)
            kolonlar = list(tablo_kolonlari(tablo_tipi))
            if with_id and "id" in df.columns:
                kolonlar = ["id"] + kolonlar
            for col in kolonlar:
                if col not in df.columns:
                    df[col] = None
            return df[kolonlar]
    except Exception:
        log.exception("verileri_yukle hata proje=%s tip=%s", proje_adi, tablo_tipi)
    cols = tablo_kolonlari(tablo_tipi)
    if with_id:
        cols = ["id"] + list(cols)
    return pd.DataFrame(columns=cols)


def verileri_yukle_dortlu(proje_adi, limit, with_id=False):
    """Aynı proje için dört finans kategorisini paralel çeker (soğuk önbellekte gidiş-dönüş süresini kısaltır)."""
    tipler = FINANS_TIPLERI
    out = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(verileri_yukle, proje_adi, tip, limit, with_id) for tip in tipler]
        for tip, fut in zip(tipler, futures):
            out[tip] = fut.result()
    return out


@st.cache_data(ttl=CACHE_DATA_TTL, show_spinner=False)
def get_project_id(proje_adi):
    try:
        res = (
            supabase.table("projects")
            .select("id")
            .eq("project_name", proje_adi)
            .eq("is_active", True)
            .limit(1)
            .execute()
        )
        if res.data:
            return res.data[0]["id"]
    except Exception:
        log.exception("get_project_id hata proje=%s", proje_adi)
    return None


@st.cache_data(ttl=CACHE_DATA_TTL, show_spinner=False)
def aktif_proje_listesi():
    try:
        res = supabase.table("projects").select("project_name").eq("is_active", True).order("project_name").execute()
        return [r["project_name"] for r in res.data] if res.data else []
    except Exception:
        log.exception("aktif_proje_listesi hata")
        return []


def cache_temizle():
    verileri_yukle.clear()
    aktif_proje_listesi.clear()
    get_project_id.clear()


def proje_olustur_veya_aktifleştir(proje_adi: str):
    """
    projects.project_name UNIQUE olduğu için:
    - kayıt yoksa: insert (is_active=True)
    - pasif kayıt varsa: is_active=True yap (yeniden aç)
    - aktif kayıt varsa: 'already_active'
    """
    try:
        res = supabase.table("projects").select("id,is_active").eq("project_name", proje_adi).limit(1).execute()
        if not res.data:
            supabase.table("projects").insert({"project_name": proje_adi, "is_active": True}).execute()
            return "inserted", None
        row = res.data[0]
        if row.get("is_active"):
            return "already_active", None
        supabase.table("projects").update({"is_active": True}).eq("id", row["id"]).execute()
        return "reactivated", None
    except Exception as e:
        return "error", str(e)


def merkezden_kayit_ekle(proje_adi, kategori, yeni_satir, goster_bildirim=True):
    izinli = {
        "Alacaklar": {"tarih", "aciklama", "borc", "alacak", "bakiye"},
        "Genel Giderler": {"tarih", "harcamayi_yapan", "harcama_adi", "tutar"},
        "Malzeme Giderleri": {"bolum", "tarih", "firma", "aciklama", "odeme", "kullanilan_malzeme", "marka", "temin_edilen_firma", "odenecek_tutar"},
        "İşçilik Giderleri": {"odeme_tarihi", "taseron_odemeleri", "aciklama", "tutar", "odenecek_tutar"},
    }
    rec = {k: v for k, v in dict(yeni_satir).items() if k in izinli.get(kategori, set())}
    rec = kayit_sanitize(rec)
    rec["proje_adi"] = proje_adi
    rec["tip"] = kategori
    pid = get_project_id(proje_adi)
    if pid is not None:
        rec["project_id"] = pid

    for _ in range(max(2, len(rec))):
        try:
            supabase.table("finans_verileri").insert([rec]).execute()
            cache_temizle()
            if goster_bildirim:
                st.success("✅ Kayıt buluta işlendi.")
            return True
        except Exception as e:
            err = str(e)
            if "satır düzeyinde güvenlik politikasını ihlal ediyor" in err or "row-level security policy" in err.lower():
                st.error("Bulut kayıt hatası: Supabase RLS insert izni engelliyor (code 42501).")
                return False
            if "Could not find the '" in err and "' column of 'finans_verileri'" in err:
                kolon = err.split("Could not find the '", 1)[1].split("' column", 1)[0]
                rec.pop(kolon, None)
                continue
            st.error(f"Bulut kayıt hatası: {e}")
            return False
    st.error("Bulut kayıt hatası: Kolon eşleşmesi yapılamadı.")
    return False


def proje_kar_hesapla(proje_adi):
    dfs = verileri_yukle_dortlu(proje_adi, DEFAULT_VIEW_LIMIT, False)
    alacak_df = dfs["Alacaklar"]
    genel_df = dfs["Genel Giderler"]
    malzeme_df = dfs["Malzeme Giderleri"]
    iscilik_df = dfs["İşçilik Giderleri"]

    gelir = 0.0
    gider = 0.0
    if not alacak_df.empty:
        alacak_df["borc"] = pd.to_numeric(alacak_df["borc"], errors="coerce").fillna(0)
        alacak_df["alacak"] = pd.to_numeric(alacak_df["alacak"], errors="coerce").fillna(0)
        gelir += float(alacak_df["borc"].sum())
        gider += float(alacak_df["alacak"].sum())
    if not genel_df.empty:
        genel_df["tutar"] = pd.to_numeric(genel_df["tutar"], errors="coerce").fillna(0)
        gider += float(genel_df["tutar"].sum())
    if not malzeme_df.empty:
        malzeme_df["odeme"] = pd.to_numeric(malzeme_df["odeme"], errors="coerce").fillna(0)
        malzeme_df["odenecek_tutar"] = pd.to_numeric(malzeme_df["odenecek_tutar"], errors="coerce").fillna(0)
        gider += float(malzeme_df["odenecek_tutar"].sum())
    if not iscilik_df.empty:
        iscilik_df["tutar"] = pd.to_numeric(iscilik_df["tutar"], errors="coerce").fillna(0)
        if "odenecek_tutar" in iscilik_df.columns:
            iscilik_df["odenecek_tutar"] = pd.to_numeric(iscilik_df["odenecek_tutar"], errors="coerce").fillna(0)
        else:
            iscilik_df["odenecek_tutar"] = 0.0
        ac = (
            iscilik_df["aciklama"].fillna("").astype(str).str.strip()
            if "aciklama" in iscilik_df.columns
            else pd.Series([""] * len(iscilik_df))
        )
        m_an = ac == "ANLAŞILAN İŞÇİLİK BEDELİ"
        anlasilan = float(iscilik_df.loc[m_an, "odenecek_tutar"].sum())
        gelir -= anlasilan
        m_gider = ~m_an
        gider += float(iscilik_df.loc[m_gider, "odenecek_tutar"].sum())
    return gelir - gider


def toplam_is_bedeli_ve_giderler(proje_adi):
    """Hesap ekstresi: brüt anlaşmadan düşülen ANLAŞILAN tutarı = yalnızca ödenecek_tutar; işçilik gideri = ANLAŞILAN hariç satırların ödenecek_tutar toplamı."""
    dfs = verileri_yukle_dortlu(proje_adi, 10_000, False)
    a = dfs["Alacaklar"]
    gg = dfs["Genel Giderler"]
    mg = dfs["Malzeme Giderleri"]
    ig = dfs["İşçilik Giderleri"]

    brut_anlasma = float(pd.to_numeric(a["borc"], errors="coerce").fillna(0).sum()) if not a.empty else 0.0
    gen_top = float(pd.to_numeric(gg["tutar"], errors="coerce").fillna(0).sum()) if not gg.empty else 0.0
    if not mg.empty:
        mal_top = (
            float(pd.to_numeric(mg["odenecek_tutar"], errors="coerce").fillna(0).sum())
            if "odenecek_tutar" in mg.columns
            else 0.0
        )
    else:
        mal_top = 0.0

    anlasilan_top = 0.0
    isc_top = 0.0
    if not ig.empty:
        isc = ig.copy()
        isc["tutar"] = pd.to_numeric(isc["tutar"], errors="coerce").fillna(0)
        if "odenecek_tutar" in isc.columns:
            isc["odenecek_tutar"] = pd.to_numeric(isc["odenecek_tutar"], errors="coerce").fillna(0)
        else:
            isc["odenecek_tutar"] = 0.0
        ac = isc["aciklama"].fillna("").astype(str).str.strip() if "aciklama" in isc.columns else pd.Series([""] * len(isc))
        m_an = ac == "ANLAŞILAN İŞÇİLİK BEDELİ"
        anlasilan_top = float(isc.loc[m_an, "odenecek_tutar"].sum())
        m_gider = ~m_an
        isc_top = float(isc.loc[m_gider, "odenecek_tutar"].sum())

    toplam_is_net = brut_anlasma - anlasilan_top
    toplam_gider = gen_top + mal_top + isc_top
    guncel_kar = toplam_is_net - toplam_gider
    return {
        "brut_anlasma_bedeli": brut_anlasma,
        "anlasilan_iscilik_toplam": anlasilan_top,
        "toplam_is_bedeli": toplam_is_net,
        "genel_gider": gen_top,
        "malzeme_gider": mal_top,
        "iscilik_gider": isc_top,
        "toplam_gider": toplam_gider,
        "guncel_kar_zarar": guncel_kar,
    }


def proje_excel_olustur(proje_adi):
    dfs = verileri_yukle_dortlu(proje_adi, DEFAULT_VIEW_LIMIT, False)
    sheets = {
        "Alacaklar": dfs["Alacaklar"],
        "Genel Giderler": dfs["Genel Giderler"],
        "Malzeme Giderleri": dfs["Malzeme Giderleri"],
        "İşçilik Giderleri": dfs["İşçilik Giderleri"],
    }
    gelir = 0.0
    gider = 0.0
    if not sheets["Alacaklar"].empty:
        a = sheets["Alacaklar"].copy()
        a["borc"] = pd.to_numeric(a["borc"], errors="coerce").fillna(0)
        a["alacak"] = pd.to_numeric(a["alacak"], errors="coerce").fillna(0)
        gelir = float(a["borc"].sum())
        gider += float(a["alacak"].sum())
    if not sheets["Genel Giderler"].empty:
        g = sheets["Genel Giderler"].copy()
        g["tutar"] = pd.to_numeric(g["tutar"], errors="coerce").fillna(0)
        gider += float(g["tutar"].sum())
    if not sheets["Malzeme Giderleri"].empty:
        m = sheets["Malzeme Giderleri"].copy()
        m["odenecek_tutar"] = pd.to_numeric(m["odenecek_tutar"], errors="coerce").fillna(0)
        gider += float(m["odenecek_tutar"].sum())
    anlasilan_excel = 0.0
    if not sheets["İşçilik Giderleri"].empty:
        i = sheets["İşçilik Giderleri"].copy()
        i["tutar"] = pd.to_numeric(i["tutar"], errors="coerce").fillna(0)
        if "odenecek_tutar" in i.columns:
            i["odenecek_tutar"] = pd.to_numeric(i["odenecek_tutar"], errors="coerce").fillna(0)
        else:
            i["odenecek_tutar"] = 0.0
        ac = i["aciklama"].fillna("").astype(str).str.strip() if "aciklama" in i.columns else pd.Series([""] * len(i))
        m_an = ac == "ANLAŞILAN İŞÇİLİK BEDELİ"
        anlasilan_excel = float(i.loc[m_an, "odenecek_tutar"].sum())
        gelir -= anlasilan_excel
        m_gider = ~m_an
        gider += float(i.loc[m_gider, "odenecek_tutar"].sum())

    sheets["Hesap Ekstresi"] = pd.DataFrame(
        [
            {"GENEL DURUM": "BRÜT ANLAŞMA (ALACAKLAR BORÇ)", "TUTAR": float(gelir + anlasilan_excel)},
            {"GENEL DURUM": "ANLAŞILAN İŞÇİLİK (ÖDENECEK TUTAR, GELİRDEN DÜŞÜLÜR)", "TUTAR": -anlasilan_excel},
            {"GENEL DURUM": "NET GELİR / İŞ BEDELİ", "TUTAR": gelir},
            {"GENEL DURUM": "TOPLAM GİDER (KDV DAHİL)", "TUTAR": gider},
            {"GENEL DURUM": "KALAN PARA", "TUTAR": gelir - gider},
        ]
    )

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for name, df in sheets.items():
            out_df = df
            if name == "İşçilik Giderleri" and not df.empty and "taseron_odemeleri" in df.columns:
                out_df = df.rename(columns={"taseron_odemeleri": "Taşeron Adı"})
            out_df.to_excel(writer, sheet_name=name, index=False)
    output.seek(0)
    return output.getvalue()


def _secret_optional(name: str) -> Optional[str]:
    if name not in st.secrets:
        return None
    v = st.secrets[name]
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _omega_access_gate() -> None:
    """APP_PASSWORD secrets'ta tanımlıysa paneli basit parola ile korur."""
    pwd = _secret_optional("APP_PASSWORD")
    if not pwd:
        return
    if st.session_state.get("_omega_access_ok"):
        return
    max_fails = 5
    lockout_sec = 60
    lock_until = st.session_state.get("_omega_lockout_until")
    if lock_until is not None and time.time() < float(lock_until):
        kalan = max(0, int(float(lock_until) - time.time()))
        st.title("Omega Panel — giriş")
        st.warning(f"Çok fazla hatalı deneme. Lütfen {kalan} sn sonra tekrar deneyin.")
        st.stop()
    if lock_until is not None and time.time() >= float(lock_until):
        st.session_state.pop("_omega_lockout_until", None)
        st.session_state.pop("_omega_login_fails", None)

    st.title("Omega Panel — giriş")
    st.caption("Bu ortam uygulama parolası ile korunuyor.")
    entered = st.text_input("Parola", type="password", key="_omega_pw")
    if st.button("Giriş yap", key="_omega_login", use_container_width=True):
        if hmac.compare_digest(entered.encode("utf-8"), pwd.encode("utf-8")):
            st.session_state._omega_access_ok = True
            st.session_state.pop("_omega_login_fails", None)
            st.session_state.pop("_omega_lockout_until", None)
            st.rerun()
        fails = int(st.session_state.get("_omega_login_fails", 0)) + 1
        st.session_state._omega_login_fails = fails
        if fails >= max_fails:
            st.session_state._omega_lockout_until = time.time() + lockout_sec
            st.error(f"{max_fails} hatalı deneme. {lockout_sec} sn bekleyin.")
            st.rerun()
        else:
            st.error(f"Parola doğru değil. ({fails}/{max_fails})")
    st.stop()


_omega_access_gate()

if "projeler" not in st.session_state:
    st.session_state.projeler = aktif_proje_listesi()
if "secilen_sayfa" not in st.session_state:
    st.session_state.secilen_sayfa = "🏠 ÖZET"
if "silinecek_proje" not in st.session_state:
    st.session_state.silinecek_proje = ""
if "wizard_malzeme_rows" not in st.session_state:
    st.session_state.wizard_malzeme_rows = []
if "excel_cache" not in st.session_state:
    st.session_state.excel_cache = {}


with st.sidebar:
    st.title("🏗️ OMEGA PANEL")
    if _secret_optional("APP_PASSWORD") and st.session_state.get("_omega_access_ok"):
        if st.button("🔒 Çıkış", use_container_width=True, key="_omega_logout"):
            st.session_state.pop("_omega_access_ok", None)
            st.session_state.pop("_omega_login_fails", None)
            st.session_state.pop("_omega_lockout_until", None)
            st.rerun()
    if st.button("🏠 ÖZET EKRANI", use_container_width=True):
        st.session_state.secilen_sayfa = "🏠 ÖZET"
        st.rerun()
    if st.button("➕ YENİ PROJE EKLE", use_container_width=True):
        st.session_state.secilen_sayfa = "➕ Yeni Proje Ekle"
        st.rerun()
    if st.button("🧭 VERİ GİRİŞ MERKEZİ", use_container_width=True):
        st.session_state.secilen_sayfa = "🧭 Veri Giriş Merkezi"
        st.rerun()

    st.write("### 📁 PROJELER")
    for p_name in st.session_state.projeler:
        with st.expander(f"🏢 {p_name}"):
            for sayfa in ["Alacaklar", "Genel Giderler", "Malzeme Giderleri", "İşçilik Giderleri", "Hesap Ekstresi"]:
                if st.button(sayfa, key=f"{p_name}_{sayfa}", use_container_width=True):
                    st.session_state.secilen_sayfa = f"{sayfa} ({p_name})"
                    st.rerun()


if st.session_state.secilen_sayfa == "🏠 ÖZET":
    st.title("🏠 GENEL ÖZET")
    for p in st.session_state.projeler:
        c1, c2, c3, c4 = st.columns([6, 2, 2, 1])
        with c1:
            st.info(f"🔹 {p}")
        with c2:
            if st.button("📦 EXCEL HAZIRLA", key=f"excel_hazirla_{p}", use_container_width=True):
                st.session_state.excel_cache[p] = proje_excel_olustur(p)
                st.success("Excel hazır.")
        with c3:
            if p in st.session_state.excel_cache:
                st.download_button(
                    "📤 EXCEL İNDİR",
                    st.session_state.excel_cache[p],
                    file_name=f"{p}_Tum_Tablolar.xlsx",
                    key=f"excel_indir_{p}",
                    use_container_width=True,
                )
            else:
                st.caption("Önce hazırla")
        with c4:
            if st.button("🗑️", key=f"sil_{p}", use_container_width=True):
                st.session_state.silinecek_proje = p

    if st.session_state.silinecek_proje:
        st.warning(f"'{st.session_state.silinecek_proje}' projesini silmek üzeresiniz. Emin misiniz?")
        k1, k2 = st.columns(2)
        with k1:
            if st.button("✅ EVET, SİL", use_container_width=True):
                p = st.session_state.silinecek_proje
                try:
                    supabase.table("finans_verileri").delete().eq("proje_adi", p).execute()
                    supabase.table("projects").update({"is_active": False}).eq("project_name", p).execute()
                    cache_temizle()
                    st.session_state.projeler = [x for x in st.session_state.projeler if x != p]
                    st.session_state.excel_cache.pop(p, None)
                    st.success(f"{p} silindi.")
                except Exception as e:
                    st.error(f"Proje silme hatası: {e}")
                st.session_state.silinecek_proje = ""
                st.rerun()
        with k2:
            if st.button("❌ VAZGEÇ", use_container_width=True):
                st.session_state.silinecek_proje = ""
                st.rerun()

elif st.session_state.secilen_sayfa == "➕ Yeni Proje Ekle":
    st.title("➕ Yeni Proje Ekle")
    if "_flash_proje_ok" in st.session_state:
        _pn = st.session_state.pop("_flash_proje_ok")
        st.success(f"{_pn} başarıyla oluşturuldu.")
    st.caption("Bu ekrandaki tutar alanları **KDV dahil** olarak girilmelidir.")
    proje_adi = st.text_input("Proje İsmi").upper().strip()
    st.caption("İş bedeli (Alacaklar) — **KDV dahil**")
    is_bedeli = st.number_input("İşin Bedeli (Alacaklar) — KDV dahil (₺)", min_value=0.0, value=0.0, step=1000.0, format="%.2f")

    st.markdown("### Kullanılacak Malzemeler")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        w_malzeme = st.text_input("Kullanılacak Malzeme", key="wiz_malzeme")
    with c2:
        w_marka = st.text_input("Marka", key="wiz_marka")
    with c3:
        w_firma = st.text_input("Temin Edilen Firma", key="wiz_firma")
    with c4:
        w_tutar = st.number_input("Ödenecek Tutar — KDV dahil (₺)", min_value=0.0, value=0.0, step=1000.0, format="%.2f", key="wiz_tutar", help="KDV dahil")

    if st.button("➕ MALZEME LİSTESİNE EKLE", use_container_width=True):
        if w_malzeme or w_marka or w_firma or w_tutar > 0:
            st.session_state.wizard_malzeme_rows.append(
                {
                    "kullanilan_malzeme": w_malzeme,
                    "marka": w_marka,
                    "temin_edilen_firma": w_firma,
                    "odenecek_tutar": float(w_tutar),
                }
            )
            for k in ("wiz_malzeme", "wiz_marka", "wiz_firma", "wiz_tutar"):
                if k in st.session_state:
                    del st.session_state[k]
            st.rerun()
        else:
            st.warning("En az bir malzeme alanı doldurun.")

    st.dataframe(pd.DataFrame(st.session_state.wizard_malzeme_rows), use_container_width=True, hide_index=True)

    st.markdown("### İşçilik Giderleri")
    st.caption("İşçilik için anlaşılan tutar (**KDV dahil**). Tabloda **ANLAŞILAN İŞÇİLİK BEDELİ** satırı olarak kaydedilir.")
    wiz_isc_tip = st.radio(
        "İşçilik bedeli türü",
        ["Ödenen ücret", "Ödenecek ücret"],
        horizontal=True,
        key="wiz_isc_tip",
    )
    anlasilan_iscilik = st.number_input(
        "İşçilik bedeli — KDV dahil (₺)",
        min_value=0.0,
        value=0.0,
        step=1000.0,
        format="%.2f",
        key="wiz_anlasilan_iscilik",
        help="KDV dahil",
    )

    if st.button("🚀 PROJEYİ OLUŞTUR", use_container_width=True):
        if not proje_adi:
            st.warning("Proje ismi zorunlu.")
        else:
            st.session_state.projeler = aktif_proje_listesi()
            if proje_adi in st.session_state.projeler:
                st.warning("Bu isimde aktif bir proje zaten var. Farklı bir isim seçin.")
            else:
                try:
                    durum, hata = proje_olustur_veya_aktifleştir(proje_adi)
                    if durum == "error":
                        st.error(f"Proje oluşturma hatası: {hata}")
                    elif durum == "already_active":
                        cache_temizle()
                        st.session_state.projeler = aktif_proje_listesi()
                        st.warning("Bu isimde aktif bir proje kaydı bulundu. Liste yenilendi; farklı bir isim seçin.")
                    else:
                        cache_temizle()
                        st.session_state.projeler = aktif_proje_listesi()
                        temiz_ok = True
                        if durum == "reactivated":
                            try:
                                supabase.table("finans_verileri").delete().eq("proje_adi", proje_adi).execute()
                                cache_temizle()
                            except Exception as e:
                                st.error(f"Eski finans kayıtları temizlenemedi: {e}")
                                temiz_ok = False
                        if not temiz_ok:
                            pass
                        else:
                            ok_all = True
                            if is_bedeli > 0:
                                ok_all = ok_all and merkezden_kayit_ekle(
                                    proje_adi,
                                    "Alacaklar",
                                    {
                                        "tarih": str(date.today()),
                                        "aciklama": "İŞ BEDELİ (SİHİRBAZ)",
                                        "borc": float(is_bedeli),
                                        "alacak": 0.0,
                                        "bakiye": float(is_bedeli),
                                    },
                                    goster_bildirim=False,
                                )
                            if anlasilan_iscilik > 0:
                                if wiz_isc_tip == "Ödenen ücret":
                                    itut, iodn = float(anlasilan_iscilik), 0.0
                                else:
                                    itut, iodn = 0.0, float(anlasilan_iscilik)
                                ok_all = ok_all and merkezden_kayit_ekle(
                                    proje_adi,
                                    "İşçilik Giderleri",
                                    {
                                        "odeme_tarihi": str(date.today()),
                                        "taseron_odemeleri": "ANLAŞILAN BEDEL",
                                        "aciklama": "ANLAŞILAN İŞÇİLİK BEDELİ",
                                        "tutar": itut,
                                        "odenecek_tutar": iodn,
                                    },
                                    goster_bildirim=False,
                                )
                            for row in st.session_state.wizard_malzeme_rows:
                                ok_all = ok_all and merkezden_kayit_ekle(
                                    proje_adi,
                                    "Malzeme Giderleri",
                                    {
                                        "bolum": "kullanilan",
                                        "tarih": "",
                                        "firma": "",
                                        "aciklama": "",
                                        "odeme": 0.0,
                                        "kullanilan_malzeme": row.get("kullanilan_malzeme", ""),
                                        "marka": row.get("marka", ""),
                                        "temin_edilen_firma": row.get("temin_edilen_firma", ""),
                                        "odenecek_tutar": float(row.get("odenecek_tutar", 0) or 0),
                                    },
                                    goster_bildirim=False,
                                )
                            st.session_state.wizard_malzeme_rows = []
                            if ok_all:
                                st.session_state.projeler = aktif_proje_listesi()
                                st.session_state["_flash_proje_ok"] = proje_adi
                                st.rerun()
                            else:
                                st.error("Proje oluşturulamadı.")
                except Exception as e:
                    st.error(f"Proje oluşturma hatası: {e}")

elif st.session_state.secilen_sayfa == "🧭 Veri Giriş Merkezi":
    st.title("🧭 Veri Giriş Merkezi")
    if not st.session_state.projeler:
        st.info("Önce proje oluşturun.")
    else:
        proje_adi = st.selectbox("Proje", st.session_state.projeler)
        kategori = st.selectbox("Kategori", ["Alacaklar", "Genel Giderler", "Malzeme Giderleri", "İşçilik Giderleri"])

        if kategori == "Alacaklar":
            tarih = st.date_input("Tarih", key="m_tarih_a")
            aciklama = st.text_input("Açıklama", key="m_aciklama_a")
            borc = st.number_input("Borç", min_value=0.0, value=0.0, step=1000.0, format="%.2f", key="m_borc")
            alacak = st.number_input("Alacak", min_value=0.0, value=0.0, step=1000.0, format="%.2f", key="m_alacak")
            if st.button("✅ KAYDI İŞLE", key="m_save_a", use_container_width=True):
                if aciklama.strip() == "":
                    st.warning("Açıklama zorunlu.")
                else:
                    ok = merkezden_kayit_ekle(
                        proje_adi,
                        "Alacaklar",
                        {"tarih": str(tarih), "aciklama": aciklama, "borc": float(borc), "alacak": float(alacak), "bakiye": float(borc - alacak)},
                    )
                    if ok:
                        st.toast(f"Güncel kâr: ₺ {format_tutar_tr(proje_kar_hesapla(proje_adi))}", icon="💹")

        elif kategori == "Genel Giderler":
            tarih = st.date_input("Tarih", key="m_tarih_g")
            yapan = st.selectbox("Harcamayı Yapan", ["Cansin", "Mustafa"], key="m_yapan")
            harcama_adi = st.text_input("Harcama Adı", key="m_harcama_adi")
            tutar = st.number_input("Tutar", min_value=0.0, value=0.0, step=1000.0, format="%.2f", key="m_tutar_g")
            if st.button("✅ KAYDI İŞLE", key="m_save_g", use_container_width=True):
                if harcama_adi.strip() == "" or tutar <= 0:
                    st.warning("Harcama adı ve tutar zorunlu.")
                else:
                    ok = merkezden_kayit_ekle(
                        proje_adi,
                        "Genel Giderler",
                        {"tarih": str(tarih), "harcamayi_yapan": yapan, "harcama_adi": harcama_adi, "tutar": float(tutar)},
                    )
                    if ok:
                        st.toast(f"Güncel kâr: ₺ {format_tutar_tr(proje_kar_hesapla(proje_adi))}", icon="💹")

        elif kategori == "Malzeme Giderleri":
            bolum = st.selectbox("Bölüm", ["odeme", "kullanilan"], key="m_bolum")
            if bolum == "odeme":
                tarih = st.date_input("Tarih", key="m_tarih_mo")
                firma = st.text_input("Firma", key="m_firma_mo")
                aciklama = st.text_input("Açıklama", key="m_aciklama_mo")
                odeme = st.number_input("Ödeme", min_value=0.0, value=0.0, step=1000.0, format="%.2f", key="m_odeme_mo")
                if st.button("✅ KAYDI İŞLE", key="m_save_mo", use_container_width=True):
                    if firma.strip() == "" or aciklama.strip() == "" or odeme <= 0:
                        st.warning("Firma, açıklama ve ödeme zorunlu.")
                    else:
                        ok = merkezden_kayit_ekle(
                            proje_adi,
                            "Malzeme Giderleri",
                            {
                                "bolum": "odeme",
                                "tarih": str(tarih),
                                "firma": firma,
                                "aciklama": aciklama,
                                "odeme": float(odeme),
                                "kullanilan_malzeme": "",
                                "marka": "",
                                "temin_edilen_firma": "",
                                "odenecek_tutar": 0.0,
                            },
                        )
                        if ok:
                            st.toast(f"Güncel kâr: ₺ {format_tutar_tr(proje_kar_hesapla(proje_adi))}", icon="💹")
            else:
                malzeme = st.text_input("Kullanılan Malzeme", key="m_malzeme_mk")
                marka = st.text_input("Marka", key="m_marka_mk")
                temin_firma = st.text_input("Temin Edilen Firma", key="m_temin_mk")
                odenecek = st.number_input("Ödenecek Tutar", min_value=0.0, value=0.0, step=1000.0, format="%.2f", key="m_odenecek_mk")
                if st.button("✅ KAYDI İŞLE", key="m_save_mk", use_container_width=True):
                    if malzeme.strip() == "" or marka.strip() == "" or temin_firma.strip() == "" or odenecek <= 0:
                        st.warning("Malzeme, marka, temin firma ve tutar zorunlu.")
                    else:
                        ok = merkezden_kayit_ekle(
                            proje_adi,
                            "Malzeme Giderleri",
                            {
                                "bolum": "kullanilan",
                                "tarih": "",
                                "firma": "",
                                "aciklama": "",
                                "odeme": 0.0,
                                "kullanilan_malzeme": malzeme,
                                "marka": marka,
                                "temin_edilen_firma": temin_firma,
                                "odenecek_tutar": float(odenecek),
                            },
                        )
                        if ok:
                            st.toast(f"Güncel kâr: ₺ {format_tutar_tr(proje_kar_hesapla(proje_adi))}", icon="💹")

        elif kategori == "İşçilik Giderleri":
            odeme_tarihi = st.date_input("Ödeme Tarihi", key="m_tarih_i")
            taseron = st.text_input("Taşeron Adı", key="m_taseron_i")
            aciklama = st.text_input("Açıklama", key="m_aciklama_i")
            m_isc_tip = st.radio(
                "Ücret türü",
                ["Ödenen ücret", "Ödenecek ücret"],
                horizontal=True,
                key="m_isc_tip",
            )
            tutar_in = st.number_input("Tutar — KDV dahil (₺)", min_value=0.0, value=0.0, step=1000.0, format="%.2f", key="m_tutar_i")
            if st.button("✅ KAYDI İŞLE", key="m_save_i", use_container_width=True):
                if taseron.strip() == "" or aciklama.strip() == "" or tutar_in <= 0:
                    st.warning("Taşeron adı, açıklama ve tutar zorunlu.")
                else:
                    if m_isc_tip == "Ödenen ücret":
                        tt, od = float(tutar_in), 0.0
                    else:
                        tt, od = 0.0, float(tutar_in)
                    ok = merkezden_kayit_ekle(
                        proje_adi,
                        "İşçilik Giderleri",
                        {
                            "odeme_tarihi": str(odeme_tarihi),
                            "taseron_odemeleri": taseron,
                            "aciklama": aciklama,
                            "tutar": tt,
                            "odenecek_tutar": od,
                        },
                    )
                    if ok:
                        st.toast(f"Güncel kâr: ₺ {format_tutar_tr(proje_kar_hesapla(proje_adi))}", icon="💹")

elif " (" in st.session_state.secilen_sayfa:
    parca = st.session_state.secilen_sayfa.split(" (")
    kategori = parca[0]
    p_adi = parca[1].replace(")", "")
    st.title(f"📊 {kategori}")
    st.subheader(f"Proje: {p_adi}")
    st.info("Bu sayfa görüntüleme içindir. Yeni kayıt için '🧭 Veri Giriş Merkezi'ni kullanın.")

    if kategori == "Hesap Ekstresi":
        st.info("KDV farkı bu oturumda saklanır (sayfa yenilenince sıfırlanır).")
        oz = toplam_is_bedeli_ve_giderler(p_adi)
        st.metric("Güncel Kar/Zarar", format_tutar_tl_plain(oz["guncel_kar_zarar"]))
        st.markdown("---")
        st.caption("KDV farkı varsa lütfen giriniz")
        kdv_key = f"kdv_fark_input__{p_adi}".replace(" ", "_")
        kdv_fark = st.number_input("KDV farkı (₺)", min_value=0.0, value=0.0, step=100.0, format="%.2f", key=kdv_key)
        kar_kdv_ile = float(oz["guncel_kar_zarar"]) - float(kdv_fark)
        iscilik_bedeli = float(oz["anlasilan_iscilik_toplam"]) + float(oz["iscilik_gider"])
        toplam_maliyet = float(oz["genel_gider"]) + float(oz["malzeme_gider"]) + iscilik_bedeli
        xdf = pd.DataFrame(
            [
                {"Kalem": "Anlaşma bedeli", "Tutar (₺)": float(oz["brut_anlasma_bedeli"])},
                {"Kalem": "İşçilik Bedeli", "Tutar (₺)": iscilik_bedeli},
                {"Kalem": "Genel gider", "Tutar (₺)": float(oz["genel_gider"])},
                {"Kalem": "Malzeme gideri", "Tutar (₺)": float(oz["malzeme_gider"])},
                {"Kalem": "Toplam Maliyet", "Tutar (₺)": toplam_maliyet},
                {"Kalem": "Güncel Kar/Zarar", "Tutar (₺)": float(oz["guncel_kar_zarar"])},
                {"Kalem": "KDV farkı (giriş)", "Tutar (₺)": float(kdv_fark)},
                {"Kalem": "KDV ile hesaplanan Kar/Zarar", "Tutar (₺)": kar_kdv_ile},
            ]
        )
        st.dataframe(hesap_ekstresi_ozet_stili(xdf), use_container_width=True, hide_index=True)
    else:
        df = verileri_yukle(p_adi, kategori, with_id=True)
        disp = df.drop(columns=["id"], errors="ignore") if "id" in df.columns else df
        if kategori == "İşçilik Giderleri" and not disp.empty and "taseron_odemeleri" in disp.columns:
            disp = disp.rename(columns={"taseron_odemeleri": "Taşeron Adı"})

        if not df.empty and "id" in df.columns:
            sil_key = f"fin_sil__{p_adi}__{kategori}".replace(" ", "_")
            df_widget_key = f"df_sel_{sil_key}"
            sig_df = inspect.signature(st.dataframe)
            tablo_satir_secimi = "on_select" in sig_df.parameters and "selection_mode" in sig_df.parameters

            sec_rid = None

            if tablo_satir_secimi:
                st.caption(
                    "Tabloda silinecek satıra tıklayın; seçilen satır tabloda vurgulanır. "
                    "Ardından **Seçileni sil** ile devam edin."
                )
                event = st.dataframe(
                    disp,
                    use_container_width=True,
                    hide_index=True,
                    key=df_widget_key,
                    on_select="rerun",
                    selection_mode="single-row",
                )
                sel_rows = []
                if event is not None:
                    if isinstance(event, dict):
                        sel = event.get("selection") or {}
                        sel_rows = list(sel.get("rows") or [])
                    else:
                        sel = getattr(event, "selection", None)
                        if sel is not None:
                            sel_rows = list(getattr(sel, "rows", None) or [])
                if not sel_rows and df_widget_key in st.session_state:
                    ws = st.session_state[df_widget_key]
                    if isinstance(ws, dict):
                        sel_rows = list((ws.get("selection") or {}).get("rows") or [])
                sec_idx = int(sel_rows[0]) if sel_rows else None
                if sec_idx is not None and 0 <= sec_idx < len(df):
                    sec_rid = int(df.iloc[sec_idx]["id"])
                    row = disp.iloc[sec_idx]
                    parcalar = []
                    for c in disp.columns:
                        v = row.get(c, "")
                        if v is not None and str(v).strip() != "":
                            parcalar.append(f"{c}: {v}")
                    ozet = (" · ".join(parcalar))[:220] if parcalar else f"id={sec_rid}"
                    st.caption(f"Seçili satır: {ozet}")
            else:
                etiket = {}
                for _, row in df.iterrows():
                    rid = int(row["id"])
                    parcalar = []
                    for c in disp.columns:
                        v = row.get(c, "")
                        if v is not None and str(v).strip() != "":
                            parcalar.append(f"{c}: {v}")
                    etiket[rid] = (" | ".join(parcalar))[:160] if parcalar else str(rid)
                SIL_NONE = "__silme_secimi_yok__"
                id_list = [int(x) for x in df["id"].tolist()]
                rad_opts = [SIL_NONE] + [str(int(x)) for x in id_list]

                def _rad_fmt(x):
                    if x == SIL_NONE:
                        return "—"
                    return etiket.get(int(x), str(x))

                st.caption("Streamlit sürümünüz tablo tıklama seçimini desteklemiyor; satırı aşağıdaki radyo listesinden seçin.")
                sec_rid_str = st.radio(
                    "satır",
                    options=rad_opts,
                    format_func=_rad_fmt,
                    key=f"rad_{sil_key}",
                    horizontal=False,
                    label_visibility="collapsed",
                )
                st.dataframe(disp, use_container_width=True, hide_index=True)
                if sec_rid_str != SIL_NONE:
                    sec_rid = int(sec_rid_str)

            if st.button("Seçileni sil", key=f"btn_sil_{sil_key}", use_container_width=True):
                if sec_rid is None:
                    st.warning("Önce tabloda (veya listede) silinecek satırı seçin.")
                else:
                    st.session_state[f"{sil_key}_pending"] = int(sec_rid)
            pend_k = f"{sil_key}_pending"
            if pend_k in st.session_state:
                st.warning("Seçilen satırı silmek istediğinize emin misiniz?")
                cev1, cev2 = st.columns(2)
                with cev1:
                    if st.button("Evet, sil", key=f"evet_{sil_key}", use_container_width=True):
                        try:
                            supabase.table("finans_verileri").delete().eq("id", int(st.session_state[pend_k])).execute()
                            cache_temizle()
                        except Exception as e:
                            st.error(f"Silme hatası: {e}")
                        del st.session_state[pend_k]
                        st.rerun()
                with cev2:
                    if st.button("Vazgeç", key=f"vaz_{sil_key}", use_container_width=True):
                        del st.session_state[pend_k]
                        st.rerun()
        else:
            st.dataframe(disp, use_container_width=True, hide_index=True)

        if kategori == "Alacaklar" and not disp.empty:
            tb = float(pd.to_numeric(disp["borc"], errors="coerce").fillna(0).sum())
            ta = float(pd.to_numeric(disp["alacak"], errors="coerce").fillna(0).sum())
            bal = tb - ta
            c_a, c_b, c_c = st.columns(3)
            with c_a:
                st.metric("Toplam Borç", f"₺ {format_tutar_tr(tb)}")
            with c_b:
                st.metric("Toplam Alacak", f"₺ {format_tutar_tr(ta)}")
            with c_c:
                st.metric("Bakiye (Borç − Alacak)", f"₺ {format_tutar_tr(bal)}")

        if kategori == "Genel Giderler" and not disp.empty:
            tg = float(pd.to_numeric(disp["tutar"], errors="coerce").fillna(0).sum())
            st.metric("Gelen Gider Toplamı", f"₺ {format_tutar_tr(tg)}")

        if kategori == "Malzeme Giderleri" and not disp.empty:
            o1 = float(pd.to_numeric(disp["odeme"], errors="coerce").fillna(0).sum()) if "odeme" in disp.columns else 0.0
            o2 = (
                float(pd.to_numeric(disp["odenecek_tutar"], errors="coerce").fillna(0).sum())
                if "odenecek_tutar" in disp.columns
                else 0.0
            )
            kalan_malz = o2 - o1
            st.metric("Malzeme Toplam Gider (yalnızca Ödenecek tutar toplamı)", f"₺ {format_tutar_tr(o2)}")
            st.caption(
                f"Ödenecek tutar sütunu toplamı − Ödeme sütunu toplamı: "
                f"₺ {format_tutar_tr(o2)} − ₺ {format_tutar_tr(o1)}"
            )
            st.metric("Kalan bakiye (Ödenecek toplamı − Ödeme toplamı)", f"₺ {format_tutar_tr(kalan_malz)}")

        if kategori == "İşçilik Giderleri" and not disp.empty:
            t1 = pd.to_numeric(disp["tutar"], errors="coerce").fillna(0).sum()
            if "odenecek_tutar" in disp.columns:
                t2 = pd.to_numeric(disp["odenecek_tutar"], errors="coerce").fillna(0).sum()
            else:
                t2 = 0.0
            kalan_isc = float(t2) - float(t1)
            st.metric("Kalan bakiye (Ödenecek − Ödenen)", f"₺ {format_tutar_tr(kalan_isc)}")
            st.caption(f"Ödenecek toplam: ₺ {format_tutar_tr(t2)} · Ödenen toplam: ₺ {format_tutar_tr(t1)}")
        xdf = disp

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        xdf.to_excel(writer, index=False)
    st.download_button("📥 EXCEL İNDİR", output.getvalue(), f"{p_adi}_{kategori}.xlsx", use_container_width=True)
