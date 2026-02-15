# streamlit_app.py
import streamlit as st
import polars as pl
from datetime import datetime

# Page config
st.set_page_config(
    page_title="BundesFeed",
    page_icon="📜",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    /* Main container */
    .main {
        background-color: #DAE0E6;
    }
    
    /* Post card */
    .post-card {
        background-color: white;
        border: 1px solid #ccc;
        border-radius: 4px;
        padding: 16px;
        margin-bottom: 10px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        transition: all 0.2s ease;
    }
    
    
    /* Status badges */
    .status-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
        margin-right: 8px;
    }
    
    .status-bearbeitung {
        background-color: #0079D3;
        color: white;
    }
    
    .status-geschafft {
        background-color: #46D160;
        color: white;
    }
    
    .status-erfolgreich {
        background-color: #00A86B;
        color: white;
    }
    
    .status-gescheitert {
        background-color: #FF4500;
        color: white;
    }
    
    .status-abgebrochen {
        background-color: #7C7C7C;
        color: white;
    }
    
    .status-unklar {
        background-color: #FFB000;
        color: white;
    }
    
    /* Title */
    .post-title {
        font-size: 18px;
        font-weight: 600;
        color: #1c1c1c;
        margin-bottom: 8px;
        line-height: 1.3;
    }
    
    /* Abstract */
    .post-abstract {
        color: #7c7c7c;
        font-size: 14px;
        line-height: 1.5;
        margin-bottom: 12px;
    }
    
    /* Metadata */
    .post-meta {
        font-size: 12px;
        color: #787C7E;
        margin-bottom: 8px;
    }
    
    /* Tags */
    .tag {
        display: inline-block;
        background-color: #EDEFF1;
        color: #1c1c1c;
        padding: 4px 8px;
        border-radius: 2px;
        font-size: 12px;
        font-weight: 500;
        margin-right: 4px;
        margin-bottom: 4px;
    }
    
    /* Detail view */
    .detail-header {
        font-size: 24px;
        font-weight: 700;
        margin-bottom: 16px;
        color: #1c1c1c;
    }
    
    /* Timeline */
    .timeline-item {
        border-left: 3px solid #0079D3;
        padding-left: 16px;
        margin-bottom: 20px;
        position: relative;
    }
    
    .timeline-icon {
        position: absolute;
        left: -14px;
        background: white;
        font-size: 20px;
    }
    
    .timeline-date {
        font-size: 12px;
        color: #7c7c7c;
        font-weight: 600;
    }
    
    .timeline-title {
        font-size: 16px;
        font-weight: 600;
        margin: 4px 0;
    }
    
    .timeline-detail {
        font-size: 14px;
        color: #4a4a4a;
        margin-top: 4px;
    }
</style>
""", unsafe_allow_html=True)

# Load data
@st.cache_data
def load_vorgaenge():
    df = pl.read_parquet("db/gesetzgebung_21.parquet")
    return df.collect() if hasattr(df, 'collect') else df

@st.cache_data
def load_vorgangspositionen():
    df = pl.read_parquet("db/vorgangsposition_gesetzgebung_21.parquet")
    return df.collect() if hasattr(df, 'collect') else df

def format_date(date_val):
    """Format date to German style"""
    if not date_val:
        return "Kein Datum"
    try:
        if isinstance(date_val, str):
            dt = datetime.fromisoformat(str(date_val).replace('Z', '+00:00'))
        else:
            dt = date_val
        return dt.strftime("%d.%m.%Y")
    except:
        return str(date_val)

def get_status_class(status):
    """Get CSS class for status"""
    status_map = {
        "In Bearbeitung": "bearbeitung",
        "Fast geschafft": "geschafft",
        "Erfolgreich": "erfolgreich",
        "Gescheitert": "gescheitert",
        "Abgebrochen": "abgebrochen",
        "Unklar": "unklar"
    }
    return status_map.get(status, "unklar")

def get_timeline_icon(position: str) -> str:
    """Icon basierend auf Vorgangsposition"""
    if not position:
        return "📄"
    
    position_lower = position.lower()
    
    if "gesetzentwurf" in position_lower:
        return "📝"
    elif "1. beratung" in position_lower or "1. durchgang" in position_lower:
        return "👥"
    elif "beschlussempfehlung" in position_lower:
        return "📋"
    elif "2. beratung" in position_lower:
        return "💬"
    elif "3. beratung" in position_lower:
        return "🗳️"
    elif "verkündung" in position_lower or "verkündet" in position_lower:
        return "📜"
    elif "überweisung" in position_lower:
        return "➡️"
    else:
        return "📄"

def render_timeline(vorgang_id: str):
    """Render timeline for a specific Vorgang"""
    df_pos = load_vorgangspositionen()
    
    # Filter by vorgang_id
    timeline = df_pos.filter(pl.col("vorgang_id") == vorgang_id).sort("datum")
    
    if len(timeline) == 0:
        st.info("Keine Vorgangspositionen verfügbar")
        return
    
    st.markdown("### 📅 Zeitverlauf")
    
    for row in timeline.iter_rows(named=True):
        icon = get_timeline_icon(row['vorgangsposition'])
        datum = format_date(row['datum'])
        
        timeline_html = f"""
        <div class="timeline-item">
            <div class="timeline-icon">{icon}</div>
            <div class="timeline-date">{datum} • {row.get('zuordnung', 'N/A')}</div>
            <div class="timeline-title">{row['vorgangsposition']}</div>
        """
        
        # Details hinzufügen
        details = []
        
        if row.get('dokumentart'):
            details.append(f"📄 {row['dokumentart']}")
        
        if row.get('drucksachetyp'):
            details.append(f"📑 {row['drucksachetyp']}")
        
        if row.get('dokumentnummer'):
            details.append(f"Nr. {row['dokumentnummer']}")

        if row.get('pdf_url'):
            details.append(f"<a href='{row['pdf_url']}' target='_blank' style='color: #0079D3; text-decoration: none;'>📄 Dokument</a>")
        
        if details:
            timeline_html += f"<div class='timeline-detail'>{' • '.join(details)}</div>"
        
        if row.get('abstract'):
            abstract_clean = row['abstract'][:200] + "..." if len(row['abstract']) > 200 else row['abstract']
            timeline_html += f"<div class='timeline-detail' style='margin-top: 8px;'>{abstract_clean}</div>"
        
        timeline_html += "</div>"
        
        st.markdown(timeline_html, unsafe_allow_html=True)
        
        #st.markdown("---")

def render_post(row, clickable=True):
    """Render a single post card"""
    status_class = get_status_class(row['status'])
    
    # Clean abstract
    abstract = row.get('abstract_clean') or row.get('abstract') or "Keine Beschreibung verfügbar"
    if len(abstract) > 400:
        abstract = abstract[:400] + "..."
    
    # Format tags
    tags = row.get('tags', []) or []
    tags_html = ""
    if tags and len(tags) > 0:
        tags_html = "".join([f'<span class="tag">#{tag}</span>' for tag in tags[:5]])
    
    # Format sachgebiete
    sachgebiete = row.get('sachgebiet', []) or []
    sachgebiet_html = ""
    if sachgebiete and len(sachgebiete) > 0:
        sachgebiet_html = "".join([f'<span class="tag">📂 {sg}</span>' for sg in sachgebiete])
    
    # Format initiative
    initiativen = row.get('initiative', []) or []
    initiative_str = ", ".join(initiativen) if initiativen else "Unbekannt"
    
    datum = format_date(row.get('datum'))
    
    tags_section = ""
    if tags_html or sachgebiet_html:
        tags_section = f"<div>{tags_html}{sachgebiet_html}</div>"
    
    html = f"""
    <div class="post-card" id="vorgang-{row['id']}">
        <div>
            <span class="status-badge status-{status_class}">{row['status']}</span>
            <span class="post-meta">📅 {datum} • 👤 {initiative_str}</span>
        </div>
        <div class="post-title">{row['titel']}</div>
        <div class="post-abstract">{abstract}</div>
        {tags_section}
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)
    
    # Make clickable
    if clickable:
        if st.button("📖 Details anzeigen", key=f"btn_{row['id']}", use_container_width=True):
            st.session_state.selected_vorgang = row['id']
            st.rerun()

def show_detail_view(vorgang_id: str):
    """Show detail view for a specific Vorgang"""
    df = load_vorgaenge()
    
    # Get the Vorgang
    vorgang = df.filter(pl.col("id") == vorgang_id)
    
    if len(vorgang) == 0:
        st.error("Vorgang nicht gefunden")
        return
    
    row = vorgang.to_dicts()[0]
    print(row)
    
    # Back button
    if st.button("← Zurück zur Übersicht"):
        del st.session_state.selected_vorgang
        st.rerun()
    
    st.markdown("---")
    
    # Status Badge
    status_class = get_status_class(row['status'])
    st.markdown(f"""
    <div style="margin-bottom: 20px;">
        <span class="status-badge status-{status_class}">{row['status']}</span>
    </div>
    """, unsafe_allow_html=True)
    
    # Title
    st.markdown(f"<div class='detail-header'>{row['titel']}</div>", unsafe_allow_html=True)
    
    # Metadata
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Datum", format_date(row.get('datum')))
    with col2:
        initiativen = row.get('initiative', []) or []
        st.metric("Initiative", ", ".join(initiativen) if initiativen else "Unbekannt")
    with col3:
        st.metric("Wahlperiode", row.get('wahlperiode', 'N/A'))
    
    # Abstract
    if row.get('abstract'):
        st.markdown("### 📄 Beschreibung")
        st.html(row['abstract'])
    
    # Beratungsstand:
    st.markdown("### 🏛️ Beratungsstand")
    beratungsstand = row.get('beratungsstand', 'Unbekannt')
    st.markdown(f"<div style='font-size: 16px;'>{beratungsstand}</div>", unsafe_allow_html=True)

    # Tags & Sachgebiete
    col1, col2 = st.columns(2)
    with col1:
        tags = row.get('tags', []) or []
        if tags and len(tags) > 0:
            st.markdown("### 🏷️ Tags")
            for tag in tags:
                st.markdown(f"`{tag}`")
    
    with col2:
        sachgebiete = row.get('sachgebiet', []) or []
        if sachgebiete and len(sachgebiete) > 0:
            st.markdown("### 📂 Sachgebiete")
            for sg in sachgebiete:
                st.markdown(f"- {sg}")
    
    st.markdown("---")
    
    # Timeline
    render_timeline(vorgang_id)

def main():
    # Initialize session state
    if 'selected_vorgang' not in st.session_state:
        st.session_state.selected_vorgang = None
    
    if 'page' not in st.session_state:
        st.session_state.page = 1
    
    # Show detail view or list view
    if st.session_state.selected_vorgang:
        show_detail_view(st.session_state.selected_vorgang)
        return
    
    # === LIST VIEW ===
    
    # Header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("📜 BundesFeed")
    
    # Load data
    df = load_vorgaenge()
    
    # Initialize session state for filters
    if 'selected_statuses' not in st.session_state:
        st.session_state.selected_statuses = df['status'].unique().to_list()
    
    if 'selected_sachgebiete' not in st.session_state:
        st.session_state.selected_sachgebiete = []
    
    # Sidebar filters
    with st.sidebar:
        st.header("Filter")
        
        # Status filter
        all_statuses = df['status'].unique().to_list()
        selected_statuses = st.multiselect(
            "Status",
            options=all_statuses,
            default=all_statuses,
            key="status_filter"
        )
        st.session_state.selected_statuses = selected_statuses
        
        # Sachgebiet filter
        all_sachgebiete = set()
        for sg_list in df['sachgebiet'].to_list():
            if sg_list:
                all_sachgebiete.update(sg_list)
        
        selected_sachgebiete = st.multiselect(
            "Sachgebiet",
            options=sorted(all_sachgebiete),
            default=st.session_state.selected_sachgebiete
        )
        st.session_state.selected_sachgebiete = selected_sachgebiete
        
        # Sort
        sort_by = st.selectbox(
            "Sortierung",
            options=["Neueste zuerst", "Älteste zuerst"],
            index=0
        )
        
        # Reset button
        if st.button("🔄 Filter zurücksetzen"):
            st.session_state.selected_statuses = all_statuses
            st.session_state.selected_sachgebiete = []
            st.session_state.page = 1
            st.rerun()
    
    # Filter data
        # Filter data
    if selected_statuses:
        filtered_df = df.filter(pl.col('status').is_in(selected_statuses))
    else:
        filtered_df = df
    
    if st.session_state.selected_sachgebiete:
        # Filter by sachgebiet (at least one match)
        mask = pl.Series([False] * len(filtered_df))
        for idx, row in enumerate(filtered_df.iter_rows(named=True)):
            if row['sachgebiet']:
                if any(sg in st.session_state.selected_sachgebiete for sg in row['sachgebiet']):
                    mask[idx] = True
        filtered_df = filtered_df.filter(mask)
    
    # Sort
    if sort_by == "Neueste zuerst":
        filtered_df = filtered_df.sort('datum', descending=True)
    else:
        filtered_df = filtered_df.sort('datum', descending=False)
    
    # Pagination
    POSTS_PER_PAGE = 10
    total_posts = len(filtered_df)
    total_pages = max(1, (total_posts + POSTS_PER_PAGE - 1) // POSTS_PER_PAGE)
    
    # Ensure page is in valid range
    if st.session_state.page > total_pages:
        st.session_state.page = total_pages
    if st.session_state.page < 1:
        st.session_state.page = 1
    
    # Display info
    st.markdown(f"**{total_posts}** Gesetze gefunden")
    
    # Get current page data
    start_idx = (st.session_state.page - 1) * POSTS_PER_PAGE
    end_idx = min(start_idx + POSTS_PER_PAGE, total_posts)
    page_df = filtered_df[start_idx:end_idx]
    
    # Render posts
    if len(page_df) == 0:
        st.info("Keine Gesetze gefunden. Passe deine Filter an.")
    else:
        for row in page_df.iter_rows(named=True):
            render_post(row, clickable=True)
    
    # Pagination controls
    if total_pages > 1:
        st.markdown("<div style='margin-top: 40px;'></div>", unsafe_allow_html=True)
        
        col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
        
        with col1:
            if st.button("⏮️ Erste", disabled=st.session_state.page == 1):
                st.session_state.page = 1
                st.rerun()
        
        with col2:
            if st.button("◀️ Zurück", disabled=st.session_state.page == 1):
                st.session_state.page -= 1
                st.rerun()
        
        with col3:
            st.markdown(
                f"<div style='text-align: center; padding-top: 8px;'>"
                f"Seite {st.session_state.page} von {total_pages}</div>",
                unsafe_allow_html=True
            )
        
        with col4:
            if st.button("Weiter ▶️", disabled=st.session_state.page == total_pages):
                st.session_state.page += 1
                st.rerun()
        
        with col5:
            if st.button("Letzte ⏭️", disabled=st.session_state.page == total_pages):
                st.session_state.page = total_pages
                st.rerun()

if __name__ == "__main__":
    main()