import streamlit as st
import pandas as pd
import plotly.express as px
import json
import requests
from scrape import get_auctions, get_all_items_for_auction, get_auction_pickup_dates
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import plotly.graph_objects as go
from datetime import datetime
import pytz

st.set_page_config(layout="wide", page_title="BidFTA Explorer", page_icon="🔍")

# Add custom CSS for image hover effect
st.markdown("""
<style>
[data-testid="column"] img {
    transition: transform .1s;
    cursor: pointer;
}
[data-testid="column"] img:hover {
    transform: scale(5);
    z-index: 1000;
}
</style>
""", unsafe_allow_html=True)

# Initialize session state for data persistence
if 'data' not in st.session_state:
    st.session_state.data = None
if 'show_location_selector' not in st.session_state:
    st.session_state.show_location_selector = True

@st.cache_data(ttl=3600)
def load_locations():
    url = "https://auction.bidfta.io/api/location/getAllLocations"
    headers = {
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'origin': 'https://www.bidfta.com',
        'referer': 'https://www.bidfta.com/'
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    locations = response.json()
    
    location_groups = {
        "Kentucky": [loc for loc in locations if loc.get('state') == 'KY'],
        "Ohio": [loc for loc in locations if loc.get('state') == 'OH'],
        "All Other States": [loc for loc in locations if loc.get('state') not in ['KY', 'OH', 'IN', 'TN', 'WV']]
    }
    
    for group in location_groups:
        location_groups[group].sort(key=lambda x: x.get('city', ''))
    
    return location_groups, locations

@st.cache_data(ttl=3600)
def get_default_locations(locations):
    return [loc for loc in locations if loc.get('state') in ['KY', 'OH']]

@st.cache_data(ttl=3600)
def scrape_bidfta_data(location_ids):
    session = requests.Session()
    _, locations = load_locations()
    locations = {loc['id']: loc for loc in locations}
    
    # Collect all auctions with pagination
    all_auctions = []
    page = 1
    while True:
        auctions = get_auctions(session, location_ids, page_id=page)
        if not auctions:
            break
        all_auctions.extend(auctions)
        page += 1
    
    if not all_auctions:
        return None
    
    pickup_dates_cache = {}
    all_items = []
    lock = threading.Lock()
    
    def process_auction(auction):
        auction_id = auction["id"]
        loc_id = auction.get("locationId")
        
        # Thread-safe pickup dates cache access
        with lock:
            if loc_id not in pickup_dates_cache:
                pickup_dates_cache[loc_id] = get_auction_pickup_dates(session, loc_id)
        
        loc_info = locations.get(loc_id, {})
        items = get_all_items_for_auction(session, auction_id)
        auction_items = []
        
        for item in items:
            current_bid = item.get("currentBid", 0.0)
            msrp = item.get("msrp", 0.0)
            ratio = current_bid / msrp if msrp > 0 else 0
            
            auction_items.append({
                'item_title': item.get('title', ''),
                'condition': item.get('condition', ''),
                'item_category1': item.get('category1', ''),
                'item_category2': item.get('category2', ''),
                'current_bid': current_bid,
                'msrp': msrp,
                'auction_location_nickname': loc_info.get('nickName', ''),
                'item_url': f"https://www.bidfta.com/{auction_id}/item-detail/{item.get('id')}",
                'auction_end_datetime': auction.get('utcEndDateTime', ''),
                'ratio_bid_to_msrp': ratio,
                'picture': item.get('imageUrl', '')
            })
        
        return auction_items
    
    # Show a message while processing
    with st.spinner(f'📥 Scraping data from {len(all_auctions)} auctions...'):
        # Process auctions in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_auction, auction) for auction in all_auctions]
            for future in as_completed(futures):
                all_items.extend(future.result())
    
    return pd.DataFrame(all_items) if all_items else None

def process_data(data):
    if data is None or len(data) == 0:
        return None

    # Convert auction_end_datetime to EST timezone
    est = pytz.timezone('US/Eastern')
    # First convert to pandas datetime
    data['auction_end_datetime'] = pd.to_datetime(data['auction_end_datetime'])
    # Check if already tz-aware and convert accordingly
    if data['auction_end_datetime'].dt.tz is None:
        data['auction_end_datetime'] = data['auction_end_datetime'].dt.tz_localize(pytz.UTC).dt.tz_convert(est)
    else:
        data['auction_end_datetime'] = data['auction_end_datetime'].dt.tz_convert(est)
    
    # Calculate time remaining
    now = datetime.now(est)
    data['ending_in'] = data['auction_end_datetime'].apply(lambda x: get_time_remaining(x, now))
    
    # Calculate ratio of current bid to MSRP
    data['ratio_bid_to_msrp'] = data['current_bid'] / data['msrp']
    
    return data

def get_time_remaining(end_time, now):
    if pd.isna(end_time):
        return "N/A"
    
    time_diff = end_time - now
    days = time_diff.days
    hours = time_diff.seconds // 3600
    minutes = (time_diff.seconds % 3600) // 60
    
    if days < 0 or (days == 0 and hours < 0):
        return "Ended"
    
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0 or (days == 0 and minutes > 0):
        parts.append(f"{hours}h")
    if days == 0 and hours == 0 and minutes > 0:
        parts.append(f"{minutes}m")
    if not parts:
        return "Ending soon"
        
    return " ".join(parts)

COLUMN_NAMES = {
    'picture': 'Picture',
    'item_title': 'Item Title',
    'condition': 'Condition',
    'item_category1': 'Primary Category',
    'item_category2': 'Secondary Category',
    'current_bid': 'Current Bid',
    'msrp': 'MSRP',
    'auction_location_nickname': 'Auction Location',
    'item_url': 'Item URL',
    'auction_end_datetime': 'End Date (EST)',
    'ending_in': 'Ending In',
    'ratio_bid_to_msrp': 'Bid/MSRP Ratio',
}

st.title("🔍 BidFTA Data Explorer")

st.markdown("""
<style>
    .stRadio [role=radiogroup] {
        gap: 2em;
    }
</style>
""", unsafe_allow_html=True)

# Add custom CSS for image hover effect
st.markdown("""
<style>
[data-testid="stImage"] img {
    transition: transform .2s;
}
[data-testid="stImage"] img:hover {
    transform: scale(2.5);
    z-index: 1000;
}
</style>
""", unsafe_allow_html=True)

# Show Update Data button only if we have data
if st.session_state.data is not None:
    if st.button("🔄 Update Data", use_container_width=True):
        # Clear all cached functions
        load_locations.clear()
        get_default_locations.clear()
        scrape_bidfta_data.clear()
        # Reset session state
        st.session_state.show_location_selector = True
        st.session_state.data = None
        st.rerun()

if st.session_state.show_location_selector or st.session_state.data is None:
    tab1, tab2 = st.tabs(["📊 Live Data", "📤 Upload CSV"])
    
    with tab2:
        uploaded_file = st.file_uploader("📂 Upload CSV file", type="csv")
        if uploaded_file:
            st.session_state.data = pd.read_csv(uploaded_file)
            st.session_state.show_location_selector = False
            st.rerun()
            
    with tab1:
        location_groups, locations = load_locations()
        
        # Only show location selector if we have no data or explicitly requested
        if st.session_state.data is None or st.session_state.show_location_selector:
            with st.container():
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown("### 📍 Select Locations")
                    quick_select = st.radio(
                        "🎯 Quick Select",
                        [
                            "🌟 Kentucky & Ohio",
                            "🌿 Kentucky Only",
                            "🌰 Ohio Only",
                            "✨ Custom Selection"
                        ],
                        format_func=lambda x: x.split(' ', 1)[1],
                        horizontal=True
                    )
                    
                    if "Kentucky & Ohio" in quick_select:
                        selected_locations = location_groups["Kentucky"] + location_groups["Ohio"]
                    elif "Kentucky Only" in quick_select:
                        selected_locations = location_groups["Kentucky"]
                    elif "Ohio Only" in quick_select:
                        selected_locations = location_groups["Ohio"]
                    else:
                        tabs = st.tabs([f"📍 {group}" for group in location_groups.keys()])
                        selected_locations = []
                        
                        for tab, group_name in zip(tabs, location_groups):
                            with tab:
                                locations = location_groups[group_name]
                                if not locations:
                                    st.info("No locations available in this group")
                                    continue
                                    
                                select_all = st.checkbox(f"✅ Select All {group_name}", key=f"all_{group_name}")
                                st.divider()
                                
                                cols = st.columns(2)
                                for i, loc in enumerate(locations):
                                    col_idx = i % 2
                                    with cols[col_idx]:
                                        location_key = f"loc_{loc['id']}"
                                        if select_all:
                                            st.checkbox(
                                                f"🏪 {loc['city']} - {loc['nickName']}", 
                                                key=location_key,
                                                value=True
                                            )
                                            selected_locations.append(loc)
                                        else:
                                            if st.checkbox(f"🏪 {loc['city']} - {loc['nickName']}", key=location_key):
                                                selected_locations.append(loc)
                    
                    if selected_locations:
                        with col2:
                            st.write("")
                            st.write("")
                            if st.button("🚀 Start Scraping", type="primary"):
                                location_ids = [loc['id'] for loc in selected_locations]
                                with st.spinner('📥 Scraping data...'):
                                    st.session_state.data = scrape_bidfta_data(location_ids)
                                    if st.session_state.data is None:
                                        st.warning('⚠️ No active auctions found.')
                                    else:
                                        st.success('✅ Data scraped successfully!')
                                        st.session_state.show_location_selector = False
                                        st.rerun()
        else:
            col1, col2 = st.columns([6, 1])
            with col2:
                if st.button("📍 Change Locations"):
                    st.session_state.show_location_selector = True
                    st.rerun()

data = st.session_state.data
if data is not None:
    original_data = process_data(data.copy())  

    with st.sidebar:
        st.header("🎛️ Filters & Options")
        
        # Add visualization toggle
        show_viz = st.checkbox("📊 Show Visualizations", value=False)
        st.divider()
        
        search_query = st.text_input("🔍 Search in titles", placeholder="Enter keywords...")
        
        categories1 = sorted(original_data['item_category1'].dropna().unique().tolist())
        selected_categories1 = st.multiselect(
            "📁 Primary Category",
            options=categories1
        )
        
        # Filter secondary categories based on selected primary categories
        if selected_categories1:
            filtered_data = original_data[original_data['item_category1'].isin(selected_categories1)]
        else:
            filtered_data = original_data
            
        categories2 = sorted(filtered_data['item_category2'].dropna().unique().tolist())
        selected_categories2 = st.multiselect(
            "📂 Secondary Category",
            options=categories2,
            disabled=len(selected_categories1) == 0,
            help="First select a primary category" if len(selected_categories1) == 0 else None
        )

        conditions = sorted(original_data['condition'].dropna().unique().tolist())
        selected_conditions = st.multiselect(
            "🏷️ Item Condition",
            options=conditions
        )
        
        hide_as_is = st.checkbox("🚫 Hide 'As Is' condition items", value=False, help="Exclude items marked as 'As Is' condition")

        # Create temp_data for price range calculations
        temp_data = original_data.copy()
        
        # Apply filters sequentially to get correct ranges
        if selected_categories1:
            temp_data = temp_data[temp_data['item_category1'].isin(selected_categories1)]
        if selected_categories2:
            temp_data = temp_data[temp_data['item_category2'].isin(selected_categories2)]
        if selected_conditions:
            temp_data = temp_data[temp_data['condition'].isin(selected_conditions)]
        if hide_as_is:
            temp_data = temp_data[~temp_data['condition'].fillna('').str.lower().str.contains('as is')]

        price_cols = st.columns(2)
        with price_cols[0]:
            show_msrp = st.checkbox("💰 MSRP", value=True)
        with price_cols[1]:
            show_current = st.checkbox("💵 Current Bid", value=True)

        if show_msrp:
            min_msrp = float(temp_data['msrp'].min())
            max_msrp = float(temp_data['msrp'].max())
            msrp_range = st.slider(
                "💲 MSRP Range ($)",
                min_value=min_msrp,
                max_value=max_msrp,
                value=(min_msrp, max_msrp),
                format="$%d"
            )

        if show_current:
            min_bid = float(temp_data['current_bid'].min())
            max_bid = float(temp_data['current_bid'].max())
            bid_range = st.slider(
                "💰 Current Bid Range ($)",
                min_value=min_bid,
                max_value=max_bid,
                value=(min_bid, max_bid),
                format="$%d"
            )

        filter_incomplete = st.checkbox("🔧 Hide incomplete items", value=False)
        
        default_columns = ['item_title', 'condition', 'item_category1', 'item_category2', 'current_bid', 'msrp', 'auction_location_nickname', 'item_url']
        columns = default_columns
        # columns = st.multiselect(
        #     "📝 Select columns to display",
        #     options=[(col, COLUMN_NAMES.get(col, col)) for col in data.columns],
        #     default=[(col, COLUMN_NAMES.get(col, col)) for col in default_columns],
        #     format_func=lambda x: x[1]
        # )
        # columns = [col[0] for col in columns]  

        sort_options = [(col, COLUMN_NAMES.get(col, col)) for col in data.columns]
        sort_selection = st.selectbox(
            "📈 Sort by",
            options=sort_options,
            index=sort_options.index(('msrp', 'MSRP')) if 'msrp' in data.columns else 0,
            format_func=lambda x: x[1]
        )
        sort_column = sort_selection[0]  

        sort_order = st.radio(
            "🔄 Sort order",
            options=["Descending", "Ascending"],
            horizontal=True
        )

    # Initialize filtered data
    filtered_data = original_data.copy()
    
    # Apply filters in sequence
    if selected_categories1:
        filtered_data = filtered_data[filtered_data['item_category1'].isin(selected_categories1)]
    if selected_categories2:
        filtered_data = filtered_data[filtered_data['item_category2'].isin(selected_categories2)]
    if selected_conditions:
        filtered_data = filtered_data[filtered_data['condition'].isin(selected_conditions)]
    
    # Apply Hide As Is filter
    if hide_as_is:
        filtered_data = filtered_data[~filtered_data['condition'].fillna('').str.lower().str.contains('as is')]

    # Apply price filters
    if show_msrp:
        filtered_data = filtered_data[
            (filtered_data['msrp'] >= msrp_range[0]) & 
            (filtered_data['msrp'] <= msrp_range[1])
        ]
    if show_current:
        filtered_data = filtered_data[
            (filtered_data['current_bid'] >= bid_range[0]) & 
            (filtered_data['current_bid'] <= bid_range[1])
        ]

    # Apply search filter
    if search_query:
        filtered_data = filtered_data[
            filtered_data['item_title'].str.contains(search_query, case=False, na=False)
        ]

    # Apply incomplete filter
    if filter_incomplete:
        filtered_data = filtered_data[
            ~filtered_data['item_title'].str.contains('incomplete', case=False, na=False)
        ]

    # Apply sorting
    filtered_data = filtered_data.sort_values(by=sort_column, ascending=(sort_order == "Ascending"))

    # Summary Statistics at the top
    st.header("📈 Summary Statistics")
    stats_col1, stats_col2, stats_col3, stats_col4 = st.columns(4)
    
    with stats_col1:
        st.metric("Total Items", len(filtered_data))
    with stats_col2:
        st.metric("Average MSRP", f"${filtered_data['msrp'].mean():,.2f}")
    with stats_col3:
        st.metric("Average Current Bid", f"${filtered_data['current_bid'].mean():,.2f}")
    with stats_col4:
        avg_ratio = filtered_data[filtered_data['ratio_bid_to_msrp'] > 0]['ratio_bid_to_msrp'].mean()
        st.metric("Average Bid/MSRP Ratio", f"{avg_ratio:.1%}")
    
    # Display filtered data
    st.header("📋 Filtered Data")
    
    col1, col2 = st.columns([8, 2])
    with col1:
        total_items = len(original_data)
        filtered_items = len(filtered_data)
        st.write(f"Showing {filtered_items:,} items out of {total_items:,} total")
    
    with col2:
        if not filtered_data.empty:
            st.download_button(
                "📥 Download Results",
                filtered_data[COLUMN_NAMES.keys()].to_csv(index=False).encode('utf-8'),
                "auction_items.csv",
                "text/csv",
                key='download-csv'
            )

    column_config = {
        "picture": st.column_config.ImageColumn(
            COLUMN_NAMES['picture'],
            width="small",
            help="Click to enlarge"
        ),
        "item_url": st.column_config.LinkColumn(COLUMN_NAMES['item_url']),
        "msrp": st.column_config.NumberColumn(
            COLUMN_NAMES['msrp'],
            format="$%d"
        ),
        "current_bid": st.column_config.NumberColumn(
            COLUMN_NAMES['current_bid'],
            format="$%d"
        ),
        "item_title": st.column_config.TextColumn(
            COLUMN_NAMES['item_title'],
            width="large"
        ),
        "condition": st.column_config.TextColumn(
            COLUMN_NAMES['condition'],
            width="small"
        ),
        "item_category1": st.column_config.TextColumn(
            COLUMN_NAMES['item_category1'],
            width="medium"
        ),
        "item_category2": st.column_config.TextColumn(
            COLUMN_NAMES['item_category2'],
            width="medium"
        ),
        "auction_location_nickname": st.column_config.TextColumn(
            COLUMN_NAMES['auction_location_nickname'],
            width="small"
        ),
        "auction_end_datetime": st.column_config.DatetimeColumn(
            COLUMN_NAMES['auction_end_datetime'],
            format="MMM DD, YYYY h:mm a",
            width="medium"
        ),
        "ending_in": st.column_config.TextColumn(
            COLUMN_NAMES['ending_in'],
            width="small"
        )
    }
    
    st.dataframe(
        filtered_data[COLUMN_NAMES.keys()],
        column_config=column_config,
        hide_index=True,
        height=600
    )
    
    # Only show visualizations if toggle is on
    if show_viz:
        st.divider()
        st.header("📊 Data Insights")
        
        # Category Distribution
        st.subheader("📊 Category Distribution")
        cat_col1, cat_col2 = st.columns(2)
        
        with cat_col1:
            # Primary Categories Pie Chart
            fig_primary_pie = px.pie(
                filtered_data, 
                names='item_category1',
                title='Primary Categories',
                hole=0.4
            )
            fig_primary_pie.update_traces(textposition='outside', textinfo='percent+label')
            st.plotly_chart(fig_primary_pie, use_container_width=True)
        
        with cat_col2:
            # Secondary Categories Pie Chart (only for selected primary categories)
            if selected_categories1:
                secondary_data = filtered_data[filtered_data['item_category2'].notna()]
                fig_secondary_pie = px.pie(
                    secondary_data, 
                    names='item_category2',
                    title='Secondary Categories (Selected Primary Categories)',
                    hole=0.4
                )
                fig_secondary_pie.update_traces(textposition='outside', textinfo='percent+label')
                st.plotly_chart(fig_secondary_pie, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # 1. Top Categories by Average MSRP
            st.subheader("💰 Most Valuable Categories")
            category_msrp = filtered_data.groupby('item_category1')['msrp'].agg(['mean', 'count']).reset_index()
            category_msrp = category_msrp[category_msrp['count'] >= 5]  # Filter categories with at least 5 items
            category_msrp = category_msrp.sort_values('mean', ascending=True).tail(10)
            
            fig_categories = px.bar(
                category_msrp,
                x='mean',
                y='item_category1',
                orientation='h',
                title=f"Top 10 Categories by Average MSRP",
                labels={'mean': 'Average MSRP ($)', 'item_category1': 'Category'},
                color='mean',
                color_continuous_scale='Viridis'
            )
            fig_categories.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig_categories, use_container_width=True)
        
        with col2:
            # 2. Best Deals (Lowest Bid/MSRP Ratio)
            st.subheader("🎯 Best Value Deals")
            deals_data = filtered_data[
                (filtered_data['msrp'] > 50) &  # Filter out very cheap items
                (filtered_data['ratio_bid_to_msrp'] > 0) &  # Filter valid ratios
                (filtered_data['ratio_bid_to_msrp'] < 1)    # Only items below MSRP
            ].sort_values('ratio_bid_to_msrp').head(10)
            
            fig_deals = px.bar(
                deals_data,
                x='ratio_bid_to_msrp',
                y='item_title',
                orientation='h',
                title="Top 10 Best Value Deals (Current Bid vs MSRP)",
                labels={'ratio_bid_to_msrp': 'Current Bid / MSRP', 'item_title': 'Item'},
                color='ratio_bid_to_msrp',
                color_continuous_scale='RdYlGn_r',
                hover_data=['current_bid', 'msrp', 'auction_location_nickname']
            )
            fig_deals.update_layout(showlegend=False, height=400)
            st.plotly_chart(fig_deals, use_container_width=True)
        
        # 3. Price Distribution by Location
        st.subheader("📍 Price Distribution by Location")
        location_stats = filtered_data.groupby('auction_location_nickname').agg({
            'current_bid': ['count', 'mean', 'median'],
            'msrp': 'mean'
        }).reset_index()
        location_stats.columns = ['Location', 'Item Count', 'Avg Bid', 'Median Bid', 'Avg MSRP']
        location_stats = location_stats[location_stats['Item Count'] >= 10]  # Filter locations with at least 10 items
        
        fig_locations = go.Figure(data=[
            go.Scatter(
                x=location_stats['Avg Bid'],
                y=location_stats['Avg MSRP'],
                mode='markers',
                hoverinfo='text',
                hovertext=[f"Location: {loc}<br>Avg Bid: ${bid:.2f}<br>Avg MSRP: ${msrp:.2f}" for loc, bid, msrp in zip(location_stats['Location'], location_stats['Avg Bid'], location_stats['Avg MSRP'])],
                marker=dict(
                    size=[count / 10 for count in location_stats['Item Count']],
                    color=location_stats['Median Bid'],
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title='Median Bid')
                )
            ),
            go.Scatter(
                x=[0, max(location_stats['Avg MSRP'].max(), location_stats['Avg Bid'].max())],
                y=[0, max(location_stats['Avg MSRP'].max(), location_stats['Avg Bid'].max())],
                mode='lines',
                line=dict(dash='dash', color='red', width=1),
                name='MSRP = Current Bid'
            )
        ])
        fig_locations.update_layout(
            title="Location Price Analysis",
            xaxis_title="Average Current Bid ($)",
            yaxis_title="Average MSRP ($)",
            height=500,
            hovermode='x'
        )
        st.plotly_chart(fig_locations, use_container_width=True)

    # # Display the table
    # st.write(html, unsafe_allow_html=True)
