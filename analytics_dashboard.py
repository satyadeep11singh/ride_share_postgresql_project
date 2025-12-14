"""
analytics_dashboard.py
Purpose: Generate interactive Plotly visualizations for all 15 ride-share analytics queries

Requirements:
  - pandas: Data manipulation and SQL query execution
  - plotly: Interactive visualization generation
  - psycopg2: PostgreSQL database driver
  - python-dotenv: Environment variable management

Database: PostgreSQL (ride_share_project)

Setup Instructions:
  1. Install dependencies: pip install pandas plotly psycopg2 python-dotenv
  2. Create .env file with: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME (or use defaults)
  3. Run: python analytics_dashboard.py
  4. Output: 15 HTML reports saved to docs/ folder

Generated Reports - BASIC QUERIES (Q1-Q3):
  - 01_driver_efficiency.html: Top 10 most efficient drivers (scatter plot)
  - 02_peak_hours_heatmap.html: Revenue by day/hour (heatmap)
  - 03_top_peak_hours.html: Top 20 peak hours (bar chart)
  - 04_vip_customers.html: Top 5 customers combined analysis (bar + scatter)
  - 05_vip_revenue_distribution.html: Customer revenue split (pie chart)

Generated Reports - ADVANCED QUERIES (Q4-Q15) - Window Functions:
  - 06_driver_leaderboard.html: ROW_NUMBER efficiency ranking (leaderboard)
  - 07_revenue_tier_ranks.html: RANK-based revenue tiers (bar chart)
  - 08_quality_tier_distribution.html: DENSE_RANK quality tiers (distribution)
  - 09_commission_percentiles.html: PERCENT_RANK commission allocation (percentile chart)
  - 10_driver_efficiency_quartiles.html: CUME_DIST quartile segmentation (scatter)
  - 11_customer_value_segments.html: NTILE customer segmentation (bar)
  - 12_churn_risk_matrix.html: LEAD idle time risk detection (scatter)
  - 13_driver_progression.html: FIRST_VALUE onboarding progression (line chart)
  - 14_quality_trend_alerts.html: LAST_VALUE quality degradation (bar with alerts)
  - 15_driver_milestones.html: NTH_VALUE lifecycle milestones (funnel)
"""

import sys
import psycopg2
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import os
from dotenv import load_dotenv

# Force UTF-8 output
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# Load environment variables
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "satyadeep")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pass654321")
DB_NAME = os.getenv("DB_NAME", "ride_share_project")

# Output directory for HTML files
OUTPUT_DIR = "docs"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


class AnalyticsDashboard:
    """Unified class to handle database connections and generate all analytics visualizations"""
    
    def __init__(self):
        """Initialize database connection"""
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            print("[OK] Database connection successful")
        except Exception as e:
            print(f"[FAILED] Database connection failed: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("[OK] Database connection closed")
    
    def fetch_data(self, query):
        """Execute query and return DataFrame"""
        try:
            df = pd.read_sql(query, self.conn)
            return df
        except Exception as e:
            print(f"[FAILED] Query execution failed: {e}")
            return None
    
    # =====================================================
    # BASIC QUERIES (Q1-Q3): Core Analytics
    # =====================================================
    
    # QUERY 1: Driver Utilization and Efficiency
    def get_driver_efficiency(self):
        """Fetch top 10 most efficient drivers"""
        query = """
        SELECT
            driver_name,
            AVG(avg_wait_time_minutes) AS overall_avg_wait_time_minutes,
            SUM(total_rides_that_day) AS total_rides
        FROM
            v_driver_daily_utilization
        GROUP BY
            driver_name
        HAVING
            SUM(total_rides_that_day) > 100
        ORDER BY
            overall_avg_wait_time_minutes ASC,
            total_rides DESC
        LIMIT 10;
        """
        return self.fetch_data(query)
    
    def plot_driver_efficiency(self, df):
        """Create interactive scatter plot for driver efficiency"""
        fig = px.scatter(
            df,
            x="overall_avg_wait_time_minutes",
            y="total_rides",
            size="total_rides",
            hover_name="driver_name",
            hover_data={
                "overall_avg_wait_time_minutes": ":.2f",
                "total_rides": True
            },
            color="overall_avg_wait_time_minutes",
            color_continuous_scale="RdYlGn_r",
            title="Driver Efficiency Analysis (Top 10)<br><sub>Lower wait time = Higher efficiency</sub>",
            labels={
                "overall_avg_wait_time_minutes": "Average Wait Time (minutes)",
                "total_rides": "Total Rides"
            },
            template="plotly_white"
        )
        
        fig.update_layout(
            height=600,
            hovermode="closest",
            font=dict(size=12)
        )
        
        fig.write_html(f"{OUTPUT_DIR}/01_driver_efficiency.html")
        print("[OK] Generated: 01_driver_efficiency.html")
        return fig
    
    # QUERY 2: Peak Hour Revenue Analysis
    def get_peak_hours(self):
        """Fetch peak hour revenue data"""
        query = """
        SELECT
            dd.day_name,
            EXTRACT(HOUR FROM fr.start_time) AS hour_of_day,
            COUNT(fr.ride_id) AS total_rides,
            SUM(fr.fare_amount) AS total_revenue
        FROM
            fact_rides fr
        JOIN
            dim_date dd ON fr.start_date_key = dd.date_key
        GROUP BY
            dd.day_name,
            hour_of_day
        ORDER BY
            total_revenue DESC
        LIMIT 20;
        """
        return self.fetch_data(query)
    
    def plot_peak_hours_heatmap(self, df):
        """Create heatmap for peak hour revenue"""
        # Pivot data for heatmap
        pivot_data = df.pivot_table(
            values="total_revenue",
            index="day_name",
            columns="hour_of_day",
            aggfunc="sum"
        )
        
        # Define day order for better visualization
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        pivot_data = pivot_data.reindex([d for d in day_order if d in pivot_data.index])
        
        fig = go.Figure(
            data=go.Heatmap(
                z=pivot_data.values,
                x=pivot_data.columns,
                y=pivot_data.index,
                colorscale="Viridis",
                hovertemplate="<b>%{y}</b> at %{x}:00<br>Revenue: $%{z:.2f}<extra></extra>"
            )
        )
        
        fig.update_layout(
            title="Peak Hour Revenue Heatmap<br><sub>Darker color = Higher revenue</sub>",
            xaxis_title="Hour of Day (24h)",
            yaxis_title="Day of Week",
            height=500,
            template="plotly_white"
        )
        
        fig.write_html(f"{OUTPUT_DIR}/02_peak_hours_heatmap.html")
        print("[OK] Generated: 02_peak_hours_heatmap.html")
        return fig
    
    def plot_peak_hours_bar(self, df):
        """Create bar chart for top 20 peak hours"""
        df_sorted = df.sort_values("total_revenue", ascending=True).tail(20)
        df_sorted["hour_label"] = df_sorted["day_name"] + " " + df_sorted["hour_of_day"].astype(int).astype(str) + ":00"
        
        fig = px.bar(
            df_sorted,
            x="total_revenue",
            y="hour_label",
            orientation="h",
            color="total_rides",
            hover_data={
                "total_revenue": ":.2f",
                "total_rides": True,
                "hour_label": False
            },
            color_continuous_scale="Teal",
            title="Top 20 Peak Hours by Revenue",
            labels={
                "total_revenue": "Total Revenue ($)",
                "hour_label": "Day & Hour",
                "total_rides": "Rides"
            },
            template="plotly_white"
        )
        
        fig.update_layout(height=700, hovermode="closest")
        fig.write_html(f"{OUTPUT_DIR}/03_top_peak_hours.html")
        print("[OK] Generated: 03_top_peak_hours.html")
        return fig
    
    # QUERY 3: Customer Segmentation (VIP Analysis)
    def get_vip_customers(self):
        """Fetch top 5 VIP customers"""
        query = """
        SELECT
            du.masked_name,
            COUNT(fr.ride_id) AS total_rides,
            SUM(fr.fare_amount) AS total_fare_spent,
            AVG(fr.distance_km) AS avg_ride_distance_km
        FROM
            fact_rides fr
        JOIN
            dim_user du ON fr.user_key = du.user_key
        GROUP BY
            du.masked_name
        ORDER BY
            total_fare_spent DESC,
            total_rides DESC
        LIMIT 5;
        """
        return self.fetch_data(query)
    
    def plot_vip_customers(self, df):
        """Create combined visualization for VIP customers"""
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{"type": "bar"}, {"type": "scatter"}]],
            subplot_titles=("Total Spending by Customer", "Rides vs. Average Distance")
        )
        
        # Left: Bar chart for total spending
        fig.add_trace(
            go.Bar(
                x=df["masked_name"],
                y=df["total_fare_spent"],
                name="Total Spent",
                marker_color="lightblue",
                hovertemplate="<b>%{x}</b><br>Total Spent: $%{y:.2f}<extra></extra>"
            ),
            row=1, col=1
        )
        
        # Right: Scatter for rides vs distance
        fig.add_trace(
            go.Scatter(
                x=df["total_rides"],
                y=df["avg_ride_distance_km"],
                mode="markers+text",
                name="Rides vs. Distance",
                marker=dict(
                    size=df["total_fare_spent"] / 50000,  # Size proportional to spending
                    color=df["total_fare_spent"],
                    colorscale="Plasma",
                    showscale=False,
                    line=dict(width=2, color="white")
                ),
                text=df["masked_name"],
                textposition="top center",
                hovertemplate="<b>%{text}</b><br>Rides: %{x}<br>Avg Distance: %{y:.2f} km<extra></extra>"
            ),
            row=1, col=2
        )
        
        fig.update_xaxes(title_text="Customer", row=1, col=1)
        fig.update_yaxes(title_text="Total Revenue ($)", row=1, col=1)
        fig.update_xaxes(title_text="Total Rides", row=1, col=2)
        fig.update_yaxes(title_text="Average Distance (km)", row=1, col=2)
        
        fig.update_layout(
            title_text="Top 5 VIP Customers Analysis",
            height=500,
            showlegend=False,
            template="plotly_white"
        )
        
        fig.write_html(f"{OUTPUT_DIR}/04_vip_customers.html")
        print("[OK] Generated: 04_vip_customers.html")
        return fig
    
    def plot_vip_spending_pie(self, df):
        """Create pie chart for VIP customer spending distribution"""
        # Create custom hover text with all metrics
        df['hover_text'] = (
            "<b>" + df['masked_name'] + "</b><br>" +
            "Revenue: $" + df['total_fare_spent'].round(2).astype(str) + "<br>" +
            "Rides: " + df['total_rides'].astype(str) + "<br>" +
            "Avg Distance: " + df['avg_ride_distance_km'].round(2).astype(str) + " km"
        )
        
        fig = px.pie(
            df,
            values="total_fare_spent",
            names="masked_name",
            title="Revenue Distribution: Top 5 VIP Customers",
            template="plotly_white",
            hover_data={'hover_text': True, 'total_fare_spent': False, 'total_rides': False, 'avg_ride_distance_km': False}
        )
        
        fig.update_traces(
            textposition="inside",
            textinfo="label+percent",
            hovertemplate="%{customdata}<extra></extra>",
            customdata=df['hover_text'].values
        )
        
        fig.update_layout(height=500)
        fig.write_html(f"{OUTPUT_DIR}/05_vip_revenue_distribution.html")
        print("[OK] Generated: 05_vip_revenue_distribution.html")
        return fig
    
    # =====================================================
    # ADVANCED QUERIES (Q4-Q15): Window Functions
    # =====================================================
    
    # QUERY 4: ROW_NUMBER - Driver Efficiency Leaderboard
    def get_driver_leaderboard(self):
        """Fetch driver efficiency leaderboard with ROW_NUMBER ranking"""
        query = """
        WITH driver_efficiency AS (
            SELECT
                driver_name,
                AVG(avg_wait_time_minutes) AS overall_avg_wait_time_minutes,
                SUM(total_rides_that_day) AS total_rides
            FROM v_driver_daily_utilization
            WHERE total_rides_that_day > 0
            GROUP BY driver_name
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY overall_avg_wait_time_minutes ASC) AS efficiency_rank,
            driver_name,
            overall_avg_wait_time_minutes,
            total_rides,
            ROUND((AVG(overall_avg_wait_time_minutes) OVER ())::numeric, 2) AS fleet_avg_wait_time
        FROM driver_efficiency
        ORDER BY efficiency_rank
        LIMIT 15;
        """
        return self.fetch_data(query)
    
    def plot_driver_leaderboard(self, df):
        """Create leaderboard visualization for driver efficiency"""
        fig = px.bar(
            df,
            x="overall_avg_wait_time_minutes",
            y="driver_name",
            orientation="h",
            color="efficiency_rank",
            hover_data={
                "overall_avg_wait_time_minutes": ":.2f",
                "total_rides": True,
                "fleet_avg_wait_time": ":.2f",
                "efficiency_rank": True
            },
            color_continuous_scale="RdYlGn_r",
            title="Driver Efficiency Leaderboard (Top 15) - ROW_NUMBER Ranking",
            labels={
                "overall_avg_wait_time_minutes": "Avg Wait Time (min)",
                "driver_name": "Driver",
                "efficiency_rank": "Rank"
            },
            template="plotly_white"
        )
        
        fig.update_layout(height=600, hovermode="closest")
        fig.write_html(f"{OUTPUT_DIR}/06_driver_leaderboard.html")
        print("[OK] Generated: 06_driver_leaderboard.html")
        return fig
    
    # QUERY 5: RANK - Revenue-Based Driver Tiers
    def get_revenue_tier_ranks(self):
        """Fetch revenue-based driver tier classification with RANK"""
        query = """
        WITH driver_revenue_ranks AS (
            SELECT
                dd.driver_name,
                COUNT(fr.ride_id) AS total_rides,
                SUM(fr.fare_amount) AS total_revenue,
                RANK() OVER (ORDER BY SUM(fr.fare_amount) DESC) AS revenue_rank
            FROM fact_rides fr
            JOIN dim_driver dd ON fr.driver_key = dd.driver_key
            GROUP BY dd.driver_name
        )
        SELECT
            revenue_rank,
            driver_name,
            total_rides,
            total_revenue,
            ROUND((total_revenue / total_rides)::numeric, 2) AS avg_fare_per_ride,
            CASE
                WHEN revenue_rank <= 10 THEN 'Top Tier (Rank <=10)'
                WHEN revenue_rank <= 30 THEN 'Mid Tier (Rank 11-30)'
                ELSE 'Growth Tier (Rank >30)'
            END AS performance_tier
        FROM driver_revenue_ranks
        ORDER BY
            revenue_rank
        LIMIT 30;
        """
        return self.fetch_data(query)
    
    def plot_revenue_tier_ranks(self, df):
        """Create revenue tier distribution visualization"""
        tier_colors = {
            "Top Tier (Rank <=10)": "#2ecc71",
            "Mid Tier (Rank 11-30)": "#f39c12",
            "Growth Tier (Rank >30)": "#e74c3c"
        }
        
        fig = px.bar(
            df.sort_values("revenue_rank"),
            x="driver_name",
            y="total_revenue",
            color="performance_tier",
            hover_data={
                "revenue_rank": True,
                "total_rides": True,
                "avg_fare_per_ride": ":.2f",
                "performance_tier": True
            },
            color_discrete_map=tier_colors,
            title="Driver Revenue Tiers (RANK) - Shows Tied Positions with Gaps",
            labels={
                "driver_name": "Driver",
                "total_revenue": "Total Revenue ($)",
                "performance_tier": "Tier"
            },
            template="plotly_white"
        )
        
        fig.update_layout(height=600, hovermode="closest", xaxis_tickangle=-45)
        fig.write_html(f"{OUTPUT_DIR}/07_revenue_tier_ranks.html")
        print("[OK] Generated: 07_revenue_tier_ranks.html")
        return fig
    
    # QUERY 6: DENSE_RANK - Quality Tier Distribution
    def get_quality_tier_distribution(self):
        """Fetch quality tier distribution with DENSE_RANK"""
        query = """
        WITH driver_ratings_with_rank AS (
            SELECT 
                dd.driver_name, 
                COUNT(fr.ride_id) AS rides_completed,
                ROUND(AVG(fr.average_driver_rating)::numeric, 2) AS avg_rating,
                DENSE_RANK() OVER (ORDER BY AVG(fr.average_driver_rating) DESC) AS quality_tier
            FROM fact_rides fr
            JOIN dim_driver dd ON fr.driver_key = dd.driver_key
            WHERE fr.average_driver_rating IS NOT NULL
            GROUP BY dd.driver_name
            HAVING COUNT(fr.ride_id) >= 10
        )
        SELECT
            quality_tier,
            driver_name,
            rides_completed,
            avg_rating,
            CASE
                WHEN quality_tier = 1 THEN 'Platinum (4.8+)'
                WHEN quality_tier = 2 THEN 'Gold (4.5-4.7)'
                WHEN quality_tier = 3 THEN 'Silver (4.0-4.4)'
                ELSE 'Development (Below 4.0)'
            END AS tier_name
        FROM driver_ratings_with_rank
        ORDER BY quality_tier;
        """
        return self.fetch_data(query)
    
    def plot_quality_tier_distribution(self, df):
        """Create quality tier distribution pie and bar chart"""
        tier_counts = df["tier_name"].value_counts().reset_index()
        tier_counts.columns = ["tier_name", "count"]
        
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{"type": "pie"}, {"type": "bar"}]],
            subplot_titles=("Drivers by Quality Tier", "Average Rating by Tier")
        )
        
        # Pie chart
        fig.add_trace(
            go.Pie(
                labels=tier_counts["tier_name"],
                values=tier_counts["count"],
                name="Count"
            ),
            row=1, col=1
        )
        
        # Bar chart with average ratings
        tier_avg_rating = df.groupby("tier_name")["avg_rating"].mean().reset_index()
        fig.add_trace(
            go.Bar(
                x=tier_avg_rating["tier_name"],
                y=tier_avg_rating["avg_rating"],
                name="Avg Rating",
                marker_color="lightblue",
                hovertemplate="<b>%{x}</b><br>Avg Rating: %{y:.2f}<extra></extra>"
            ),
            row=1, col=2
        )
        
        fig.update_yaxes(title_text="Average Rating", row=1, col=2)
        fig.update_layout(
            title_text="Driver Quality Tier Distribution (DENSE_RANK) - No Gaps in Numbering",
            height=500,
            showlegend=False,
            template="plotly_white"
        )
        
        fig.write_html(f"{OUTPUT_DIR}/08_quality_tier_distribution.html")
        print("[OK] Generated: 08_quality_tier_distribution.html")
        return fig
    
    # QUERY 7: PERCENT_RANK - Commission Allocation
    def get_commission_percentiles(self):
        """Fetch commission allocation by PERCENT_RANK"""
        query = """
        WITH driver_revenue_with_percentile AS (
            SELECT
                dd.driver_name,
                SUM(fr.fare_amount) AS total_revenue,
                COUNT(fr.ride_id) AS total_rides,
                ROUND((PERCENT_RANK() OVER (ORDER BY SUM(fr.fare_amount) DESC))::numeric, 3) AS revenue_percentile
            FROM fact_rides fr
            JOIN dim_driver dd ON fr.driver_key = dd.driver_key
            GROUP BY dd.driver_name
        )
        SELECT
            driver_name,
            total_revenue,
            total_rides,
            ROUND((total_revenue / NULLIF(total_rides, 0))::numeric, 2) AS revenue_per_ride,
            revenue_percentile,
            CASE
                WHEN revenue_percentile <= 0.20 THEN 'Top 20% - +15% Commission'
                WHEN revenue_percentile <= 0.50 THEN 'Top 50% - +10% Commission'
                ELSE 'Growth Pool - +5% Commission'
            END AS commission_tier
        FROM driver_revenue_with_percentile
        ORDER BY
            revenue_percentile ASC
        LIMIT 25;
        """
        return self.fetch_data(query)
    
    def plot_commission_percentiles(self, df):
        """Create commission allocation visualization"""
        tier_colors = {
            "Top 20% - +15% Commission": "#2ecc71",
            "Top 50% - +10% Commission": "#f39c12",
            "Growth Pool - +5% Commission": "#3498db"
        }
        
        fig = px.scatter(
            df,
            x="revenue_percentile",
            y="revenue_per_ride",
            size="total_revenue",
            color="commission_tier",
            hover_name="driver_name",
            hover_data={
                "revenue_percentile": ":.3f",
                "total_revenue": ":.2f",
                "total_rides": True,
                "commission_tier": True
            },
            color_discrete_map=tier_colors,
            title="Commission Allocation by PERCENT_RANK - Percentile-Based Incentives",
            labels={
                "revenue_percentile": "Revenue Percentile (0=best, 1=worst)",
                "revenue_per_ride": "Revenue per Ride ($)",
                "commission_tier": "Commission Tier"
            },
            template="plotly_white"
        )
        
        fig.update_layout(height=600, hovermode="closest")
        fig.write_html(f"{OUTPUT_DIR}/09_commission_percentiles.html")
        print("[OK] Generated: 09_commission_percentiles.html")
        return fig
    
    # QUERY 8: CUME_DIST - Efficiency Quartiles
    def get_efficiency_quartiles(self):
        """Fetch efficiency quartiles using CUME_DIST"""
        query = """
        WITH driver_efficiency_ranks AS (
            SELECT
                driver_name,
                SUM(total_rides_that_day) AS total_rides,
                AVG(avg_wait_time_minutes) AS avg_wait_time,
                ROUND((CUME_DIST() OVER (ORDER BY AVG(avg_wait_time_minutes) ASC))::numeric, 3) AS efficiency_cume_dist
            FROM v_driver_daily_utilization
            WHERE total_rides_that_day > 0
            GROUP BY driver_name
        )
        SELECT
            driver_name,
            total_rides,
            avg_wait_time,
            efficiency_cume_dist,
            CASE
                WHEN efficiency_cume_dist <= 0.25 
                    THEN 'Q1 (Best) - Fleet Stars'
                WHEN efficiency_cume_dist <= 0.50 
                    THEN 'Q2 (Good) - Reliable'
                WHEN efficiency_cume_dist <= 0.75 
                    THEN 'Q3 (Fair) - Training Eligible'
                ELSE 'Q4 (Development) - Mentoring Program'
            END AS efficiency_quartile
        FROM driver_efficiency_ranks
        ORDER BY
            efficiency_cume_dist ASC;
        """
        return self.fetch_data(query)
    
    def plot_efficiency_quartiles(self, df):
        """Create efficiency quartile distribution"""
        quartile_colors = {
            "Q1 (Best) - Fleet Stars": "#27ae60",
            "Q2 (Good) - Reliable": "#f39c12",
            "Q3 (Fair) - Training Eligible": "#e67e22",
            "Q4 (Development) - Mentoring Program": "#e74c3c"
        }
        
        fig = px.scatter(
            df,
            x="efficiency_cume_dist",
            y="avg_wait_time",
            size="total_rides",
            color="efficiency_quartile",
            hover_name="driver_name",
            hover_data={
                "efficiency_cume_dist": ":.3f",
                "avg_wait_time": ":.2f",
                "total_rides": True
            },
            color_discrete_map=quartile_colors,
            title="Driver Efficiency Quartiles (CUME_DIST) - Cumulative Distribution",
            labels={
                "efficiency_cume_dist": "Cumulative Distribution",
                "avg_wait_time": "Average Wait Time (min)",
                "efficiency_quartile": "Quartile"
            },
            template="plotly_white"
        )
        
        fig.update_layout(height=600, hovermode="closest")
        fig.write_html(f"{OUTPUT_DIR}/10_driver_efficiency_quartiles.html")
        print("[OK] Generated: 10_driver_efficiency_quartiles.html")
        return fig
    
    # QUERY 9: NTILE - Customer Value Segmentation
    def get_customer_value_segments(self):
        """Fetch customer value segments using NTILE"""
        query = """
        WITH customer_aggregates AS (
            SELECT
                du.masked_name,
                COUNT(fr.ride_id) AS total_rides,
                SUM(fr.fare_amount) AS total_fare_spent,
                AVG(fr.distance_km) AS avg_distance_km
            FROM fact_rides fr
            JOIN dim_user du ON fr.user_key = du.user_key
            GROUP BY du.masked_name
        ),
        customer_value_segments AS (
            SELECT
                masked_name,
                total_rides,
                total_fare_spent,
                avg_distance_km,
                NTILE(4) OVER (ORDER BY total_fare_spent DESC) AS customer_value_quartile
            FROM customer_aggregates
        )
        SELECT
            customer_value_quartile,
            masked_name,
            total_rides,
            total_fare_spent,
            avg_distance_km,
            CASE
                WHEN customer_value_quartile = 1 
                    THEN 'VIP (Top 25%)'
                WHEN customer_value_quartile = 2 
                    THEN 'Premium (25-50%)'
                WHEN customer_value_quartile = 3 
                    THEN 'Standard (50-75%)'
                ELSE 'Basic (Bottom 25%)'
            END AS segment_name
        FROM customer_value_segments
        ORDER BY
            customer_value_quartile, total_fare_spent DESC;
        """
        return self.fetch_data(query)
    
    def plot_customer_value_segments(self, df):
        """Create customer value segmentation visualization using NTILE quartiles.
        
        Divides customers into 4 equal-sized segments (VIP, Premium, Standard, Basic)
        based on total fare spent using SQL NTILE(4) function. Displays detailed metrics
        including fare ranges, customer count, and example names per segment.
        
        Args:
            df: DataFrame from get_customer_value_segments() with NTILE segment data
            
        Returns:
            Plotly figure with dual bar charts (customer count & total fare per segment)
        """
        # Define color scheme: green=high value → gray=low value for intuitive visualization
        segment_colors = {
            "VIP (Top 25%)": "#2ecc71",           # Green - highest value customers
            "Premium (25-50%)": "#3498db",         # Blue - high value
            "Standard (50-75%)": "#f39c12",        # Orange - medium value
            "Basic (Bottom 25%)": "#95a5a6"        # Gray - lowest value
        }
        
        # Aggregate segment statistics using multiple functions:
        # - min/max/mean/sum for fare analysis
        # - count for customer numbers and top_customers for segment examples
        segment_stats = df.groupby("segment_name").agg({
            "total_fare_spent": ["min", "max", "mean", "sum"],  # Fare distribution metrics
            "masked_name": ["count", lambda x: ", ".join(x.head(3).tolist())]  # Customer count + examples
        }).reset_index()
        
        # Flatten multi-level columns from groupby().agg()
        segment_stats.columns = ["segment_name", "min_fare", "max_fare", "avg_fare", "total_fare", "customer_count", "top_customers"]
        segment_stats = segment_stats.sort_values("customer_count", ascending=False)
        
        # Create figure with subplots
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=("Customer Count by Segment", "Total Fare Spent by Segment"),
            specs=[[{"type": "bar"}], [{"type": "bar"}]],
            vertical_spacing=0.15
        )
        
        # Build rich hover text with full segment details (different ordering for chart clarity)
        hover_text_count = []
        hover_text_fare = []
        for idx, row in segment_stats.iterrows():
            # Hover text for Customer Count chart (emphasizes number of active customers)
            count_hover = (
                f"<b>{row['segment_name']}</b><br>"
                f"Customers: {int(row['customer_count'])}<br>"
                f"Fare Range: ${row['min_fare']:,.0f} - ${row['max_fare']:,.0f}<br>"
                f"Avg Fare: ${row['avg_fare']:,.0f}<br>"
                f"Total Spent: ${row['total_fare']:,.0f}<br>"
                f"<b>Top Examples:</b><br>{row['top_customers']}"
            )
            # Hover text for Total Fare chart (emphasizes revenue contribution)
            fare_hover = (
                f"<b>{row['segment_name']}</b><br>"
                f"Total Spent: ${row['total_fare']:,.0f}<br>"
                f"Customers: {int(row['customer_count'])}<br>"
                f"Avg Fare: ${row['avg_fare']:,.0f}<br>"
                f"Fare Range: ${row['min_fare']:,.0f} - ${row['max_fare']:,.0f}<br>"
                f"<b>Top Examples:</b><br>{row['top_customers']}"
            )
            hover_text_count.append(count_hover)
            hover_text_fare.append(fare_hover)
        
        # Top chart: Customer count by segment (shows NTILE quartile distribution)
        fig.add_trace(
            go.Bar(
                x=segment_stats["segment_name"],
                y=segment_stats["customer_count"],
                name="Customers",
                marker_color=[segment_colors[seg] for seg in segment_stats["segment_name"]],
                text=segment_stats["customer_count"],
                textposition="auto",
                customdata=hover_text_count,
                hovertemplate="%{customdata}<extra></extra>"  # Show all details on hover
            ),
            row=1, col=1
        )
        
        # Bottom chart: Total fare spent by segment (shows revenue contribution)
        fig.add_trace(
            go.Bar(
                x=segment_stats["segment_name"],
                y=segment_stats["total_fare"],
                name="Total Fare ($)",
                marker_color=[segment_colors[seg] for seg in segment_stats["segment_name"]],
                text=segment_stats["total_fare"].apply(lambda x: f"${x:,.0f}"),  # Format as currency
                textposition="auto",
                customdata=hover_text_fare,
                hovertemplate="%{customdata}<extra></extra>"  # Show all details on hover
            ),
            row=2, col=1
        )
        
        fig.update_yaxes(title_text="Number of Customers", row=1, col=1)
        fig.update_yaxes(title_text="Total Fare ($)", row=2, col=1)
        fig.update_xaxes(title_text="Customer Segment", row=2, col=1)
        
        fig.update_layout(
            title_text="Customer Value Segmentation (NTILE) - Hover for Details",
            height=800,
            showlegend=False,
            template="plotly_white",
            hovermode="closest"
        )
        
        fig.write_html(f"{OUTPUT_DIR}/11_customer_value_segments.html")
        print("[OK] Generated: 11_customer_value_segments.html")
        return fig
    
    # QUERY 10: LEAD - Churn Risk Detection
    def get_churn_risk_matrix(self):
        """Fetch churn risk data using LEAD for idle time detection"""
        query = """
        WITH driver_rides_with_lead AS (
            SELECT
                dd.driver_name,
                fr.start_time AS current_ride_start,
                fr.end_time AS current_ride_end,
                fr.fare_amount AS current_fare,
                LEAD(fr.start_time) OVER (
                    PARTITION BY fr.driver_key 
                    ORDER BY fr.start_time
                ) AS next_ride_start
            FROM fact_rides fr
            JOIN dim_driver dd ON fr.driver_key = dd.driver_key
            WHERE fr.driver_key IN (1, 5, 10, 15, 20)
        )
        SELECT
            driver_name,
            DATE(current_ride_start) AS ride_date,
            EXTRACT(HOUR FROM current_ride_start) AS start_hour,
            current_fare,
            CASE
                WHEN next_ride_start IS NOT NULL
                    THEN ROUND((EXTRACT(EPOCH FROM (next_ride_start - current_ride_end)) / 3600)::numeric, 1)
                ELSE NULL
            END AS idle_hours_until_next_ride,
            CASE
                WHEN next_ride_start IS NOT NULL AND EXTRACT(EPOCH FROM (next_ride_start - current_ride_end)) / 3600 > 4 
                    THEN 'CHURN RISK: >4hr idle'
                WHEN next_ride_start IS NOT NULL AND EXTRACT(EPOCH FROM (next_ride_start - current_ride_end)) / 3600 > 2 
                    THEN 'WARNING: 2-4hr idle'
                WHEN next_ride_start IS NOT NULL
                    THEN 'Normal: <2hr idle'
                ELSE 'Last ride (no next ride data)'
            END AS retention_risk_flag
        FROM driver_rides_with_lead
        WHERE
            next_ride_start IS NOT NULL
        ORDER BY
            driver_name, current_ride_start
        LIMIT 100;
        """
        return self.fetch_data(query)
    
    def plot_churn_risk_matrix(self, df):
        """Create churn risk visualization"""
        risk_colors = {
            "CHURN RISK: >4hr idle": "#e74c3c",
            "WARNING: 2-4hr idle": "#f39c12",
            "Normal: <2hr idle": "#2ecc71"
        }
        
        fig = px.scatter(
            df,
            x="start_hour",
            y="idle_hours_until_next_ride",
            color="retention_risk_flag",
            hover_name="driver_name",
            hover_data={
                "ride_date": True,
                "current_fare": ":.2f",
                "idle_hours_until_next_ride": ":.1f",
                "retention_risk_flag": True
            },
            color_discrete_map=risk_colors,
            title="Driver Churn Risk Matrix (LEAD) - Idle Time Detection by Hour",
            labels={
                "start_hour": "Ride Start Hour",
                "idle_hours_until_next_ride": "Hours Until Next Ride",
                "retention_risk_flag": "Risk Level"
            },
            template="plotly_white"
        )
        
        fig.update_layout(height=600, hovermode="closest")
        fig.write_html(f"{OUTPUT_DIR}/12_churn_risk_matrix.html")
        print("[OK] Generated: 12_churn_risk_matrix.html")
        return fig
    
    # QUERY 11: FIRST_VALUE - Driver Onboarding Progression
    def get_driver_progression(self):
        """Fetch driver progression from first ride using FIRST_VALUE"""
        query = """
        SELECT
            driver_name,
            ride_number,
            DATE(current_ride_start) AS ride_date,
            current_fare,
            first_ever_fare,
            ROUND((current_fare - first_ever_fare)::numeric, 2) AS fare_improvement,
            ROUND((((current_fare - first_ever_fare) / NULLIF(first_ever_fare, 0)) * 100)::numeric, 1) AS improvement_percent,
            CASE
                WHEN (current_fare - first_ever_fare) >= 20 THEN 'Strong Growth (>$20 gain)'
                WHEN (current_fare - first_ever_fare) >= 5 THEN 'Positive Trend ($5-$20 gain)'
                WHEN (current_fare - first_ever_fare) >= 0 THEN 'Stable ($0-$5)'
                ELSE 'Declining (<$0)'
            END AS performance_trend
        FROM (
            SELECT
                dd.driver_name,
                fr.fare_amount AS current_fare,
                ROW_NUMBER() OVER (PARTITION BY fr.driver_key ORDER BY fr.start_time) AS ride_number,
                fr.start_time AS current_ride_start,
                FIRST_VALUE(fr.fare_amount) OVER (
                    PARTITION BY fr.driver_key
                    ORDER BY fr.start_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS first_ever_fare
            FROM fact_rides fr
            JOIN dim_driver dd ON fr.driver_key = dd.driver_key
        ) driver_progression
        WHERE
            driver_name IN ('Joann Wolfe', 'Jeremy Bautista', 'Christina Chang', 'Mark Davis', 'Brooke Snyder')
        ORDER BY
            driver_name, ride_number
        LIMIT 50;
        """
        return self.fetch_data(query)
    
    def plot_driver_progression(self, df):
        """Create driver progression line chart"""
        fig = px.line(
            df,
            x="ride_number",
            y="current_fare",
            color="driver_name",
            hover_data={
                "ride_date": True,
                "fare_improvement": ":.2f",
                "improvement_percent": ":.1f",
                "performance_trend": True
            },
            title="Driver Onboarding Progression (FIRST_VALUE) - Fare Trend from First Ride",
            labels={
                "ride_number": "Ride Number",
                "current_fare": "Fare Amount ($)",
                "driver_name": "Driver"
            },
            template="plotly_white"
        )
        
        fig.update_layout(height=600, hovermode="x unified")
        fig.write_html(f"{OUTPUT_DIR}/13_driver_progression.html")
        print("[OK] Generated: 13_driver_progression.html")
        return fig
    
    # QUERY 12: LAST_VALUE - Quality Trend Alerts
    def get_quality_trend_alerts(self):
        """Fetch quality degradation alerts using LAST_VALUE"""
        query = """
        WITH driver_quality_trends AS (
            SELECT
                fr.driver_key,
                dd.driver_name,
                COUNT(*) OVER (PARTITION BY fr.driver_key) AS total_rides,
                ROUND(AVG(fr.average_driver_rating) OVER (PARTITION BY fr.driver_key)::numeric, 2) AS lifetime_avg_rating,
                ROUND(LAST_VALUE(fr.average_driver_rating) OVER (
                    PARTITION BY fr.driver_key
                    ORDER BY fr.start_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )::numeric, 2) AS most_recent_rating,
                ROW_NUMBER() OVER (PARTITION BY fr.driver_key ORDER BY fr.start_time DESC) AS rn
            FROM fact_rides fr
            JOIN dim_driver dd ON fr.driver_key = dd.driver_key
            WHERE fr.average_driver_rating IS NOT NULL
        )
        SELECT
            driver_name,
            total_rides,
            lifetime_avg_rating,
            most_recent_rating,
            ROUND((most_recent_rating - lifetime_avg_rating)::numeric, 2) AS recent_trend,
            CASE
                WHEN (most_recent_rating - lifetime_avg_rating) < -0.5 
                    THEN 'ALERT: Quality declining >0.5 stars'
                WHEN (most_recent_rating - lifetime_avg_rating) < 0 
                    THEN 'WARNING: Slight quality decline'
                WHEN (most_recent_rating - lifetime_avg_rating) >= 0 
                    THEN 'POSITIVE: Maintaining or improving'
            END AS quality_trend_flag
        FROM driver_quality_trends
        WHERE
            rn = 1
        ORDER BY
            recent_trend ASC
        LIMIT 20;
        """
        return self.fetch_data(query)
    
    def plot_quality_trend_alerts(self, df):
        """Create quality trend alert visualization"""
        alert_colors = {
            "ALERT: Quality declining >0.5 stars": "#e74c3c",
            "WARNING: Slight quality decline": "#f39c12",
            "POSITIVE: Maintaining or improving": "#2ecc71"
        }
        
        fig = px.bar(
            df.sort_values("recent_trend"),
            x="driver_name",
            y="recent_trend",
            color="quality_trend_flag",
            hover_data={
                "lifetime_avg_rating": ":.2f",
                "most_recent_rating": ":.2f",
                "total_rides": True
            },
            color_discrete_map=alert_colors,
            title="Quality Trend Alerts (LAST_VALUE) - Recent vs Lifetime Average Rating",
            labels={
                "driver_name": "Driver",
                "recent_trend": "Rating Change (stars)",
                "quality_trend_flag": "Alert"
            },
            template="plotly_white"
        )
        
        fig.update_layout(height=600, xaxis_tickangle=-45, hovermode="closest")
        fig.write_html(f"{OUTPUT_DIR}/14_quality_trend_alerts.html")
        print("[OK] Generated: 14_quality_trend_alerts.html")
        return fig
    
    # QUERY 13: NTH_VALUE - Driver Lifecycle Milestones
    def get_driver_milestones(self):
        """Fetch driver lifecycle milestones using NTH_VALUE"""
        query = """
        WITH driver_milestones AS (
            SELECT
                fr.driver_key,
                dd.driver_name,
                COUNT(*) OVER (PARTITION BY fr.driver_key) AS total_rides_to_date,
                ROUND(NTH_VALUE(fr.fare_amount, 1) OVER (
                    PARTITION BY fr.driver_key ORDER BY fr.start_time 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )::numeric, 2) AS ride_1_fare,
                ROUND(NTH_VALUE(fr.fare_amount, 10) OVER (
                    PARTITION BY fr.driver_key ORDER BY fr.start_time 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )::numeric, 2) AS ride_10_fare,
                ROUND(NTH_VALUE(fr.fare_amount, 50) OVER (
                    PARTITION BY fr.driver_key ORDER BY fr.start_time 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )::numeric, 2) AS ride_50_fare,
                ROUND(NTH_VALUE(fr.fare_amount, 100) OVER (
                    PARTITION BY fr.driver_key ORDER BY fr.start_time 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                )::numeric, 2) AS ride_100_fare,
                ROW_NUMBER() OVER (PARTITION BY fr.driver_key ORDER BY fr.start_time DESC) AS rn
            FROM fact_rides fr
            JOIN dim_driver dd ON fr.driver_key = dd.driver_key
        )
        SELECT
            driver_name,
            total_rides_to_date,
            CASE
                WHEN total_rides_to_date >= 100 THEN 'Active (100+ rides)'
                WHEN total_rides_to_date >= 50 THEN 'Engaged (50+ rides)'
                WHEN total_rides_to_date >= 10 THEN 'Onboarded (10+ rides)'
                ELSE 'New (<10 rides)'
            END AS driver_lifecycle_stage,
            ride_1_fare,
            ride_10_fare,
            ride_50_fare,
            ride_100_fare,
            CASE
                WHEN total_rides_to_date >= 50 AND ride_10_fare > ride_1_fare
                THEN 'Strong onboarding (fares increasing)'
                ELSE 'Monitor growth pattern'
            END AS onboarding_quality
        FROM driver_milestones
        WHERE
            rn = 1 AND total_rides_to_date >= 10
        ORDER BY
            total_rides_to_date DESC
        LIMIT 30;
        """
        return self.fetch_data(query)
    
    def plot_driver_milestones(self, df):
        """Create driver lifecycle milestone visualization using NTH_VALUE analytics.
        
        Tracks driver progression through key ride milestones (1st, 10th, 50th, 100th ride)
        using NTH_VALUE window function. Shows both the retention funnel (how many drivers
        reach each milestone) and earnings progression (average fare at each milestone).
        
        Args:
            df: DataFrame from get_driver_milestones() with NTH_VALUE fare data
            
        Returns:
            Plotly figure with 2 bar charts: driver retention funnel & earnings progression
        """
        # Sort by total rides (for context, not used in visualization)
        df_sorted = df.sort_values("total_rides_to_date", ascending=False)
        
        # Create subplot figure: top for driver count funnel, bottom for earning trends
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=("Drivers Reaching Each Milestone", "Average Fare by Milestone"),
            specs=[[{"type": "bar"}], [{"type": "bar"}]],
            vertical_spacing=0.15
        )
        
        # Count how many drivers reached each milestone using NTH_VALUE data
        milestone_summary = {
            "Milestone": ["Ride #1", "Ride #10", "Ride #50", "Ride #100"],
            "Drivers Reaching": [
                len(df[df["ride_1_fare"].notna()]),    # All drivers have first ride
                len(df[df["ride_10_fare"].notna()]),   # Subset reached 10 rides
                len(df[df["ride_50_fare"].notna()]),   # Smaller subset reached 50 rides
                len(df[df["ride_100_fare"].notna()])   # Smallest subset reached 100+ rides
            ]
        }
        
        # Calculate average earnings at each milestone to show progression
        avg_fares = {
            "Ride #1": df["ride_1_fare"].mean(),     # Initial onboarding earning
            "Ride #10": df["ride_10_fare"].mean(),   # Early career average
            "Ride #50": df["ride_50_fare"].mean(),   # Mid-career typical earning
            "Ride #100": df["ride_100_fare"].mean()  # Mature driver earning potential
        }
        
        # Build hover text combining driver count and earnings metrics
        hover_text_milestones = []
        for milestone in milestone_summary["Milestone"]:
            # Get count of drivers reaching this milestone from NTH_VALUE data
            drivers_count = milestone_summary["Drivers Reaching"][milestone_summary["Milestone"].index(milestone)]
            # Get average fare at this milestone
            avg_fare = avg_fares[milestone]
            # Format hover with both retention and earnings metrics
            hover = f"<b>{milestone}</b><br>Drivers: {drivers_count}<br>Avg Fare: ${avg_fare:,.2f}"
            hover_text_milestones.append(hover)
        
        # Top chart: Funnel of drivers reaching each milestone (shows retention/persistence)
        fig.add_trace(
            go.Bar(
                x=milestone_summary["Milestone"],
                y=milestone_summary["Drivers Reaching"],
                name="Drivers",
                marker_color="#3498db",  # Blue for driver counts
                text=milestone_summary["Drivers Reaching"],
                textposition="auto",
                customdata=hover_text_milestones,
                hovertemplate="%{customdata}<extra></extra>"  # Show count + avg fare
            ),
            row=1, col=1
        )
        
        # Bottom chart: Average earning progression at each milestone (shows economics)
        avg_fares_list = [avg_fares[m] for m in milestone_summary["Milestone"]]
        fig.add_trace(
            go.Bar(
                x=milestone_summary["Milestone"],
                y=avg_fares_list,
                name="Avg Fare",
                marker_color="#2ecc71",  # Green for positive earnings trend
                text=[f"${v:,.0f}" for v in avg_fares_list],  # Format as currency
                textposition="auto",
                hovertemplate="<b>%{x}</b><br>Avg Fare: $%{y:,.2f}<extra></extra>"  # Show detailed fare
            ),
            row=2, col=1
        )
        
        fig.update_yaxes(title_text="Number of Drivers", row=1, col=1)
        fig.update_yaxes(title_text="Average Fare ($)", row=2, col=1)
        fig.update_xaxes(title_text="Milestone", row=2, col=1)
        
        fig.update_layout(
            title_text="Driver Lifecycle Milestones (NTH_VALUE) - Detailed Metrics",
            height=800,
            showlegend=False,
            template="plotly_white",
            hovermode="closest"
        )
        
        fig.write_html(f"{OUTPUT_DIR}/15_driver_milestones.html")
        print("[OK] Generated: 15_driver_milestones.html")
        return fig
    
    def generate_all_reports(self):
        """Generate all 15 visualizations (basic + advanced)"""
        print("\n" + "="*60)
        print("GENERATING UNIFIED ANALYTICS DASHBOARD (ALL 15 REPORTS)")
        print("="*60 + "\n")
        
        try:
            # BASIC REPORTS (Q1-Q3)
            print("Generating Report 1: Driver Efficiency (Q1)...")
            df_drivers = self.get_driver_efficiency()
            if df_drivers is not None and not df_drivers.empty:
                self.plot_driver_efficiency(df_drivers)
            else:
                print("[FAILED] No data for driver efficiency")
            
            print("Generating Report 2: Peak Hours Analysis (Q2)...")
            df_hours = self.get_peak_hours()
            if df_hours is not None and not df_hours.empty:
                self.plot_peak_hours_heatmap(df_hours)
                self.plot_peak_hours_bar(df_hours)
            else:
                print("[FAILED] No data for peak hours")
            
            print("Generating Report 3: VIP Customer Analysis (Q3)...")
            df_vip = self.get_vip_customers()
            if df_vip is not None and not df_vip.empty:
                self.plot_vip_customers(df_vip)
                self.plot_vip_spending_pie(df_vip)
            else:
                print("[FAILED] No data for VIP customers")
            
            # ADVANCED REPORTS (Q4-Q15) - Window Functions
            print("\nGenerating Report 4: Driver Efficiency Leaderboard (Q4 - ROW_NUMBER)...")
            df_leaderboard = self.get_driver_leaderboard()
            if df_leaderboard is not None and not df_leaderboard.empty:
                self.plot_driver_leaderboard(df_leaderboard)
            else:
                print("[FAILED] No data for driver leaderboard")
            
            print("Generating Report 5: Revenue Tier Ranks (Q5 - RANK)...")
            df_revenue_ranks = self.get_revenue_tier_ranks()
            if df_revenue_ranks is not None and not df_revenue_ranks.empty:
                self.plot_revenue_tier_ranks(df_revenue_ranks)
            else:
                print("[FAILED] No data for revenue tier ranks")
            
            print("Generating Report 6: Quality Tier Distribution (Q6 - DENSE_RANK)...")
            df_quality = self.get_quality_tier_distribution()
            if df_quality is not None and not df_quality.empty:
                self.plot_quality_tier_distribution(df_quality)
            else:
                print("[FAILED] No data for quality tier distribution")
            
            print("Generating Report 7: Commission Percentiles (Q7 - PERCENT_RANK)...")
            df_commission = self.get_commission_percentiles()
            if df_commission is not None and not df_commission.empty:
                self.plot_commission_percentiles(df_commission)
            else:
                print("[FAILED] No data for commission percentiles")
            
            print("Generating Report 8: Efficiency Quartiles (Q8 - CUME_DIST)...")
            df_quartiles = self.get_efficiency_quartiles()
            if df_quartiles is not None and not df_quartiles.empty:
                self.plot_efficiency_quartiles(df_quartiles)
            else:
                print("[FAILED] No data for efficiency quartiles")
            
            print("Generating Report 9: Customer Value Segments (Q9 - NTILE)...")
            df_segments = self.get_customer_value_segments()
            if df_segments is not None and not df_segments.empty:
                self.plot_customer_value_segments(df_segments)
            else:
                print("[FAILED] No data for customer value segments")
            
            print("Generating Report 10: Churn Risk Matrix (Q10 - LEAD)...")
            df_churn = self.get_churn_risk_matrix()
            if df_churn is not None and not df_churn.empty:
                self.plot_churn_risk_matrix(df_churn)
            else:
                print("[FAILED] No data for churn risk matrix")
            
            print("Generating Report 11: Driver Progression (Q11 - FIRST_VALUE)...")
            df_progression = self.get_driver_progression()
            if df_progression is not None and not df_progression.empty:
                self.plot_driver_progression(df_progression)
            else:
                print("[FAILED] No data for driver progression")
            
            print("Generating Report 12: Quality Trend Alerts (Q12 - LAST_VALUE)...")
            df_trends = self.get_quality_trend_alerts()
            if df_trends is not None and not df_trends.empty:
                self.plot_quality_trend_alerts(df_trends)
            else:
                print("[FAILED] No data for quality trend alerts")
            
            print("Generating Report 13: Driver Milestones (Q13 - NTH_VALUE)...")
            df_milestones = self.get_driver_milestones()
            if df_milestones is not None and not df_milestones.empty:
                self.plot_driver_milestones(df_milestones)
            else:
                print("[FAILED] No data for driver milestones")
            
            print("\n" + "="*60)
            print("[OK] ALL 15 REPORTS GENERATED SUCCESSFULLY!")
            print("[OK] Reports saved to: {}/".format(OUTPUT_DIR))
            print("="*60 + "\n")
            
        except Exception as e:
            print(f"[FAILED] Error generating reports: {e}")
        finally:
            self.close()


def main():
    """Main execution function"""
    try:
        dashboard = AnalyticsDashboard()
        dashboard.generate_all_reports()
    except Exception as e:
        print(f"[FAILED] Error: {e}")


if __name__ == "__main__":
    main()
