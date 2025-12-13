"""
08_analytics_dashboard.py
Purpose: Generate interactive Plotly visualizations from ride-share analytics queries

Requirements:
  - pandas: Data manipulation and SQL query execution
  - plotly: Interactive visualization generation
  - psycopg2: PostgreSQL database driver
  - python-dotenv: Environment variable management

Database: PostgreSQL (ride_share_project)

Setup Instructions:
  1. Install dependencies: pip install pandas plotly psycopg2 python-dotenv
  2. Create .env file with: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME (or use defaults)
  3. Run: python 08_analytics_dashboard.py
  4. Output: 5 HTML reports saved to analytics_reports/ folder

Generated Reports:
  - 01_driver_efficiency.html: Top 10 most efficient drivers (scatter plot)
  - 02_peak_hours_heatmap.html: Revenue by day/hour (heatmap)
  - 03_top_peak_hours.html: Top 20 peak hours (bar chart)
  - 04_vip_customers.html: Top 5 customers combined analysis (bar + scatter)
  - 05_vip_revenue_distribution.html: Customer revenue split (pie chart)
"""

import psycopg2
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "satyadeep")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pass654321")
DB_NAME = os.getenv("DB_NAME", "ride_share_project")

# Output directory for HTML files
OUTPUT_DIR = "analytics_reports"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


class RideShareAnalytics:
    """Class to handle database connections and generate analytics visualizations"""
    
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
            print("✓ Database connection successful")
        except Exception as e:
            print(f"✗ Database connection failed: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("✓ Database connection closed")
    
    def fetch_data(self, query):
        """Execute query and return DataFrame"""
        try:
            df = pd.read_sql(query, self.conn)
            return df
        except Exception as e:
            print(f"✗ Query execution failed: {e}")
            return None
    
    # =====================================================
    # QUERY 1: Driver Utilization and Efficiency
    # =====================================================
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
        print("✓ Generated: 01_driver_efficiency.html")
        return fig
    
    # =====================================================
    # QUERY 2: Peak Hour Revenue Analysis
    # =====================================================
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
        print("✓ Generated: 02_peak_hours_heatmap.html")
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
        print("✓ Generated: 03_top_peak_hours.html")
        return fig
    
    # =====================================================
    # QUERY 3: Customer Segmentation (VIP Analysis)
    # =====================================================
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
        print("✓ Generated: 04_vip_customers.html")
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
        print("✓ Generated: 05_vip_revenue_distribution.html")
        return fig
    
    def generate_all_reports(self):
        """Generate all visualizations"""
        print("\n" + "="*60)
        print("GENERATING ANALYTICS DASHBOARD")
        print("="*60 + "\n")
        
        try:
            # Report 1: Driver Efficiency
            print("Generating Report 1: Driver Efficiency...")
            df_drivers = self.get_driver_efficiency()
            if df_drivers is not None and not df_drivers.empty:
                self.plot_driver_efficiency(df_drivers)
            else:
                print("✗ No data for driver efficiency")
            
            # Report 2: Peak Hours
            print("\nGenerating Report 2: Peak Hours Analysis...")
            df_hours = self.get_peak_hours()
            if df_hours is not None and not df_hours.empty:
                self.plot_peak_hours_heatmap(df_hours)
                self.plot_peak_hours_bar(df_hours)
            else:
                print("✗ No data for peak hours")
            
            # Report 3: VIP Customers
            print("\nGenerating Report 3: VIP Customer Analysis...")
            df_vip = self.get_vip_customers()
            if df_vip is not None and not df_vip.empty:
                self.plot_vip_customers(df_vip)
                self.plot_vip_spending_pie(df_vip)
            else:
                print("✗ No data for VIP customers")
            
            print("\n" + "="*60)
            print(f"✓ ALL REPORTS GENERATED SUCCESSFULLY!")
            print(f"✓ Reports saved to: {OUTPUT_DIR}/")
            print("="*60 + "\n")
            
        except Exception as e:
            print(f"✗ Error generating reports: {e}")
        finally:
            self.close()


def main():
    """Main execution function"""
    try:
        analytics = RideShareAnalytics()
        analytics.generate_all_reports()
    except Exception as e:
        print(f"✗ Error: {e}")


if __name__ == "__main__":
    main()
