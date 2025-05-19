import os
import time
import json
import re
import undetected_chromedriver as uc
import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from collections import defaultdict

def run_bbp_chart_analysis(asin: str, cost: float, vat: float = 20.0, driver=None, days: int = 105) -> pd.DataFrame:
    """
    Scrapes BuyBotPro for the given ASIN, sets 'cost' and 'vat',
    and returns a DataFrame with daily pricing, ROI, scoring,
    sales simulation data, and the product Category from BBP.

    Key Features:
      - Adds 'Buy Box Price' to each daily data row
      - Averages BSR points per day (to reduce missing data)
      - Grabs the Category from xpath: //*[@id="overviewCategory"]
      - Returns final data as a DataFrame, with 'Category' and 'Buy Box Price'
    """

    simulation_results = []
    created_new_driver = False

    # ------------------------------
    # A) Setup driver if none passed
    # ------------------------------
    if driver is None:
        created_new_driver = True
        options = uc.ChromeOptions()
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--start-maximized")

        # Update these paths to match your system if needed
        options.user_data_dir = r"C:\Users\Luke\AppData\Local\Google\Chrome\User Data"
        chrome_executable = r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"

        driver = uc.Chrome(
            options=options,
            user_data_dir=options.user_data_dir,
            version_main=None,
            driver_executable_path=r"C:\Users\Luke\appdata\roaming\undetected_chromedriver\undetected_chromedriver.exe",
            browser_executable_path=chrome_executable
        )

    try:
        # ------------------------------
        # 1) LOAD AMAZON PAGE
        # ------------------------------
        url = f"https://www.amazon.co.uk/dp/{asin}"
        driver.get(url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(2)

        # ------------------------------
        # 2) SWITCH TO BBP IFRAME
        # ------------------------------
        try:
            iframe = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, '//iframe[contains(@src, "buybotpro")]'))
            )
            driver.switch_to.frame(iframe)
            print("✅ Switched into BBP iframe.")
        except Exception as e:
            print("❌ Failed to switch into BBP iframe:", e)
            if created_new_driver:
                driver.quit()
            return pd.DataFrame()

        # ------------------------------
        # 3) EXTRACT CATEGORY
        # ------------------------------
        try:
            category_elem = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="overviewCategory"]'))
            )
            category_raw = category_elem.text.strip()
            category = category_raw if category_raw else "Unknown"
            print(f"✅ Category extracted: {category}")
        except Exception as e:
            print(f"⚠️ Could not extract category: {e}")
            category = "Unknown"

        # ------------------------------
        # 4) SET VAT & COST
        # ------------------------------
        try:
            vat_field = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="txtVatRate"]'))
            )
            vat_field.clear()
            vat_field.send_keys(str(vat))
            print(f"✅ VAT set to {vat}%")
        except Exception as e:
            print("❌ Failed to set VAT:", e)

        try:
            cost_field = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="txtBuyPrice"]'))
            )
            cost_field.clear()
            cost_field.send_keys(str(cost))
            print(f"✅ Cost set to {cost}")
        except Exception as e:
            print("❌ Failed to set cost:", e)

        # ------------------------------
        # 5) EXTRACT BREAK-EVEN
        # ------------------------------
        try:
            WebDriverWait(driver, 25).until(
                EC.text_to_be_present_in_element((By.XPATH, '//*[@id="detailsBreakEven"]'), "£")
            )
            be_text = driver.find_element(By.XPATH, '//*[@id="detailsBreakEven"]').text.strip()
            match = re.search(r"£([0-9]+\.[0-9]+)", be_text)
            if match:
                break_even = float(match.group(1))
                print(f"✅ Break-even extracted: {break_even}")
            else:
                raise ValueError(f"No valid £ amount found in break-even text: '{be_text}'")
        except Exception as e:
            print("❌ Could not parse break-even:", e)
            if created_new_driver:
                driver.quit()
            return pd.DataFrame()

        # ------------------------------
        # 6) WAIT FOR BBP SALES CHART
        # ------------------------------
        print("⏳ Waiting for chart element to load...")
        chart_elem = None
        for i in range(15):
            try:
                chart_elem = driver.find_element(By.XPATH, '//*[@id="buyBotProSalesChart"]')
                if chart_elem.is_displayed():
                    print("✅ Found chart element.")
                    break
            except Exception:
                pass
            print(f"⏳ Waiting... {i+1}s")
            time.sleep(1)

        if not chart_elem:
            print("❌ Chart element not found.")
            if created_new_driver:
                driver.quit()
            return pd.DataFrame()

        # ------------------------------
        # 7) EXTRACT CHART DATA (JS)
        # ------------------------------
        def extract_chart_data(driver_ref):
            js = """
            try {
                var elem = document.getElementById("buyBotProSalesChart");
                if (!elem) return JSON.stringify({ error: "Chart element not found" });
                var chart = (typeof Chart !== 'undefined' && Chart.getChart) ? Chart.getChart(elem) : null;
                if (!chart || !chart.data) return JSON.stringify({ error: "Chart instance or data not found" });
                var labels = (chart.data.labels && chart.data.labels.length)
                             ? chart.data.labels
                             : (chart.data.datasets[0]?.data.map(item => item.x) || []);
                return JSON.stringify({
                    success: true,
                    labels: labels,
                    datasets: chart.data.datasets.map(ds => ({
                        label: ds.label,
                        data: ds.data
                    }))
                });
            } catch (e) {
                return JSON.stringify({ error: e.toString() });
            }
            """
            raw = driver_ref.execute_script(js)
            return json.loads(raw)

        chart_resp = extract_chart_data(driver)
        if not chart_resp.get("success"):
            print("❌ Failed to extract chart data:", chart_resp.get("error"))
            if created_new_driver:
                driver.quit()
            return pd.DataFrame()
        else:
            print("✅ Chart data extracted from BBP DOM.")
            labels_sample = chart_resp.get("labels", [])[:5]
            print("Labels sample:", labels_sample)
            for ds in chart_resp.get("datasets", []):
                label = ds.get("label", "")
                data_preview = ds.get("data", [])[:5]
                print(f" - {label}: {data_preview}")

        chart_data = chart_resp

        # ------------------------------
        # 8) ROI / SCORE HELPERS
        # ------------------------------
        def calculate_roi(sale_price, be):
            return ((sale_price - be) / be) * 100

        def assign_roi_score(roi):
            if roi < 0:
                return -5
            elif 0 <= roi < 5:
                return 0
            elif 5 <= roi < 10:
                return 1
            elif 10 <= roi < 20:
                return 2
            elif 20 <= roi < 35:
                return 3
            elif 35 <= roi < 60:
                return 4
            else:
                return 5

        # ------------------------------
        # 9) CAPTURE BSR (AVERAGED)
        # ------------------------------
        bsr_data = next(
            (ds.get("data", []) for ds in chart_data.get("datasets", [])
             if ds.get("label", "").strip().lower() == "bsr"),
            []
        )

        bsr_map = defaultdict(list)
        for item in bsr_data:
            try:
                ts = item["x"]
                val = item["y"]
                if ts is None or val is None:
                    print(f"⚠️ Skipped invalid BSR item: {item}")
                    continue
                day_str = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                bsr_map[day_str].append(val)
            except Exception as e:
                print(f"⚠️ Failed to process BSR item: {item}, Error: {e}")

        bsr_avg_map = {}
        for d_str, values in bsr_map.items():
            if values:
                try:
                    bsr_avg_map[d_str] = int(sum(values) / len(values))
                except Exception as e:
                    print(f"⚠️ Error averaging BSR for {d_str}: {values} — {e}")

        # ------------------------------
        # 10) CAPTURE OFFERS & REVIEWS
        # ------------------------------
        offer_count_map = {}
        review_count_map = {}
        try:
            offer_elem = driver.find_element(By.XPATH, '//*[@id="offerAndReviewsChart"]')
            js_offers = """
            try {
                const elem = document.getElementById("offerAndReviewsChart");
                if (!elem) return JSON.stringify({ error: "No offerAndReviewsChart element" });
                const chart = (typeof Chart !== 'undefined' && Chart.getChart) ? Chart.getChart(elem) : null;
                if (!chart || !chart.data) return JSON.stringify({ error: "No chart data found for Offers/Reviews" });
                return JSON.stringify({
                    success: true,
                    datasets: chart.data.datasets.map(ds => ({
                        label: ds.label,
                        data: ds.data.map(p => ({ x: p.x, y: p.y }))
                    }))
                });
            } catch (e) {
                return JSON.stringify({ error: e.toString() });
            }
            """
            raw_offers = driver.execute_script(js_offers)
            parsed_offers = json.loads(raw_offers)
            if parsed_offers.get("success"):
                print("✅ OfferAndReviewsChart data extracted!")
                for dataset in parsed_offers.get("datasets", []):
                    label = dataset.get("label", "").lower()
                    for item in dataset.get("data", []):
                        try:
                            raw_date_str = item["x"]
                            day_str = datetime.fromisoformat(raw_date_str[:10]).strftime("%Y-%m-%d")
                            if "offer" in label:
                                offer_count_map[day_str] = item["y"]
                            elif "review" in label:
                                review_count_map[day_str] = item["y"]
                        except Exception as ee:
                            print(f"⚠️ Error parsing offers/reviews item => {item}: {ee}")
            else:
                print("⚠ Offers/Reviews extraction error =>", parsed_offers.get("error"))
        except Exception as e:
            print("⚠ No #offerAndReviewsChart or extraction error =>", e)

        # ------------------------------
        # 11) IDENTIFY PRICE LINES
        # ------------------------------
        amazon_data = None
        fba_data = None
        fbm_data = None
        buy_box_data = None  # We'll store "Buy Box" data here

        for ds in chart_data.get("datasets", []):
            label_str = ds.get("label", "").strip().lower()
            if "amazon" in label_str:
                amazon_data = ds["data"]
            elif "fba" in label_str:
                fba_data = ds["data"]
            elif "fbm" in label_str:
                fbm_data = ds["data"]
            elif "buy box" in label_str:
                # This line might vary; depends on exactly how BBP labels the "Buy Box" dataset
                buy_box_data = ds["data"]

        # ------------------------------
        # 11.5) HELPER: find_closest_price
        # to get the Buy Box price at or near a timestamp
        # ------------------------------
        def find_closest_price(data_list, timestamp):
            """
            Finds the item in data_list whose 'x' is exactly
            or nearest to 'timestamp'. Returns that 'y'.
            """
            if not data_list:
                return ""
            best_diff = float("inf")
            best_price = ""
            for point in data_list:
                x_ts = point.get("x")
                y_val = point.get("y")
                if x_ts is None or y_val is None:
                    continue
                diff = abs(x_ts - timestamp)
                if diff < best_diff:
                    best_diff = diff
                    best_price = y_val
            return best_price

        # ------------------------------
        # 12) BUILD DAILY PRICE DATA
        # ------------------------------
        labels_list = chart_data.get("labels", [])
        total_points = len(labels_list)
        daily_price_data = []

        for i in range(total_points):
            chosen_price = None
            chosen_source = None
            ama_val = None
            if amazon_data and i < len(amazon_data):
                try:
                    tmp = float(amazon_data[i].get("y", 0))
                    ama_val = tmp if tmp > 0 else None
                except:
                    pass
            fba_val = None
            if fba_data and i < len(fba_data):
                try:
                    tmp = float(fba_data[i].get("y", 0))
                    fba_val = tmp if tmp > 0 else None
                except:
                    pass
            fbm_val = None
            if fbm_data and i < len(fbm_data):
                try:
                    tmp = float(fbm_data[i].get("y", 0))
                    fbm_val = tmp if tmp > 0 else None
                except:
                    pass

            candidate_list = []
            if ama_val is not None:
                candidate_list.append(("amazon", ama_val))
            if fba_val is not None:
                candidate_list.append(("fba", fba_val))
            if fbm_val is not None:
                candidate_list.append(("fbm", fbm_val))
            if not candidate_list:
                continue

            # pick the lowest price among available lines
            candidate_list.sort(key=lambda x: x[1])
            chosen_source, chosen_price = candidate_list[0]

            tstamp = labels_list[i]

            # Find the buy box price at or near that timestamp
            bb_price = find_closest_price(buy_box_data, tstamp) if buy_box_data else ""

            daily_price_data.append({
                "x": tstamp,
                "amazon_price": ama_val if ama_val else "",
                "fba_price": fba_val if fba_val else "",
                "fbm_price": fbm_val if fbm_val else "",
                "buy_box_price": bb_price,
                "chosen_source": chosen_source,
                "chosen_price": chosen_price
            })

        if not daily_price_data:
            print("❌ No valid daily price data found.")
            return pd.DataFrame()

        print("\nℹ️ Price data selected per day (showing first 10 entries):")
        for row in daily_price_data[:10]:
            print(row)

        # ------------------------------
        # 13) WEIGHTED ROI SCORING + BSR/Offers/Reviews
        # ------------------------------
        current_time_ms = time.time() * 1000
        total_weighted_score = 0.0
        total_weight = 0.0
        zone_counts = {
            "ROI < 0": 0,
            "0-5%": 0,
            "5-10%": 0,
            "10-20%": 0,
            "20-35%": 0,
            "35-60%": 0,
            ">=60%": 0
        }
        master_rows = []

        for entry in daily_price_data:
            raw_price = entry["chosen_price"]
            if raw_price <= 0:
                continue
            roi_val = calculate_roi(raw_price, break_even)
            score_val = assign_roi_score(roi_val)

            # ROI zone classification
            if roi_val < 0:
                zone_counts["ROI < 0"] += 1
            elif 0 <= roi_val < 5:
                zone_counts["0-5%"] += 1
            elif 5 <= roi_val < 10:
                zone_counts["5-10%"] += 1
            elif 10 <= roi_val < 20:
                zone_counts["10-20%"] += 1
            elif 20 <= roi_val < 35:
                zone_counts["20-35%"] += 1
            elif 35 <= roi_val < 60:
                zone_counts["35-60%"] += 1
            else:
                zone_counts[">=60%"] += 1

            tstamp = entry["x"]
            if not isinstance(tstamp, (int, float)):
                tstamp = 0
            days_ago = (time.time() * 1000 - tstamp) / (1000 * 3600 * 24) if tstamp > 0 else 0
            if entry["chosen_source"] == "amazon":
                weight = 1.0
            else:
                if days_ago <= 30:
                    weight = 1.0
                elif days_ago <= 60:
                    weight = 0.75
                elif days_ago <= 120:
                    weight = 0.5
                elif days_ago <= 180:
                    weight = 0.25
                else:
                    weight = 0.1

            total_weighted_score += score_val * weight
            total_weight += weight

            # Convert the timestamp to a date string
            day_str = str(entry["x"])
            if isinstance(entry["x"], (int, float)):
                try:
                    day_str = datetime.fromtimestamp(entry["x"] / 1000).strftime("%Y-%m-%d")
                except:
                    pass

            # BSR fallback logic
            bsr_val = bsr_avg_map.get(day_str)
            if bsr_val is None:
                try:
                    prev_day = (datetime.strptime(day_str, "%Y-%m-%d") - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                    next_day = (datetime.strptime(day_str, "%Y-%m-%d") + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                    if bsr_avg_map.get(prev_day):
                        print(f"🔁 Used previous day BSR for {day_str} => {prev_day}")
                        bsr_val = bsr_avg_map[prev_day]
                    elif bsr_avg_map.get(next_day):
                        print(f"🔁 Used next day BSR for {day_str} => {next_day}")
                        bsr_val = bsr_avg_map[next_day]
                    else:
                        print(f"⚠️ BSR missing for {day_str} and fallback days: {prev_day}, {next_day}")
                        bsr_val = ""
                except Exception as e:
                    print(f"❌ BSR fallback failed for {day_str}: {e}")
                    bsr_val = ""

            off_val = offer_count_map.get(day_str, "")
            rev_val = review_count_map.get(day_str, "")

            # Build master rows
            master_rows.append({
                "date": day_str,
                "amazon_price": entry["amazon_price"],
                "fba_price": entry["fba_price"],
                "fbm_price": entry["fbm_price"],
                "buy_box_price": entry["buy_box_price"],  # << ADDED
                "chosen_price": raw_price,
                "chosen_source": entry["chosen_source"],
                "roi_percent": roi_val,
                "roi_score": score_val,
                "bsr": bsr_val,
                "offer_count": off_val,
                "review_count": rev_val
            })

        # Summaries
        avg_roi_score = total_weighted_score / total_weight if total_weight > 0 else 0
        if avg_roi_score > 1.5:
            final_rating = "PASS"
        elif 0.5 <= avg_roi_score <= 1.5:
            final_rating = "REVIEW"
        else:
            final_rating = "FAIL"

        print("\n=== History ROI Scoring Summary (365 Days) ===")
        print(f"Total weighted score: {total_weighted_score:.2f}")
        print(f"Total weight: {total_weight:.2f}")
        print(f"Average ROI Score: {avg_roi_score:.2f}")
        print(f"Final Rating: {final_rating}")
        print("ROI Zone Distribution (% of recorded days):")
        for z, ct in zone_counts.items():
            pct = (ct / len(daily_price_data)) * 100 if daily_price_data else 0
            print(f"  {z}: {pct:.1f}%")

        print("\nDaily details (last 10 recorded entries):")
        for row in master_rows[-10:]:
            print(
                f"{row['date']} (ChosenSource: {row['chosen_source']}): "
                f"ChosenPrice = {row['chosen_price']:.2f}, "
                f"ROI = {row['roi_percent']:.2f}%, "
                f"Score = {row['roi_score']}, "
                f"BSR = {row['bsr']}, "
                f"Offers = {row['offer_count']}, "
                f"Reviews = {row['review_count']}, "
                f"BuyBox = {row['buy_box_price']}"
            )

        # ------------------------------
        # 14) EXTRACT BBP'S ESTIMATED SALES + COMPETITOR INFO
        # ------------------------------
        try:
            est_sales_elem = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="quickInfoEstSales"]'))
            )
            est_sales_text = est_sales_elem.text.strip()
            ms_match = re.search(r"(\d+)", est_sales_text)
            if ms_match:
                monthly_sales = int(ms_match.group(1))
                est_daily_sales = monthly_sales / 30.0
                print(f"✅ Estimated monthly sales extracted: {monthly_sales} (~{est_daily_sales:.2f} per day)")
            else:
                print("❌ Could not parse estimated monthly sales from:", est_sales_text)
                est_daily_sales = 0
        except Exception as e:
            print("❌ Failed to extract estimated sales:", e)
            est_daily_sales = 0

        try:
            comp_table = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="competitionAnalysisDataTable"]'))
            )
            comp_rows = comp_table.find_elements(By.XPATH, ".//tbody/tr")
            competitor_count = 0
            tolerance = 0.05
            for crow in comp_rows:
                try:
                    price_el = crow.find_element(By.XPATH, ".//td[3]/span")
                    ptxt = price_el.text.strip().replace("£", "").replace(",", "")
                    cp_val = float(ptxt)
                    seller_el = crow.find_element(By.XPATH, ".//td[1]")
                    stxt = seller_el.text.strip().lower()
                    if "fba" in stxt:
                        # compare competitor's price to our cost within 5%
                        if abs(cp_val - cost) / cost <= tolerance:
                            competitor_count += 1
                except:
                    continue
            total_sellers = competitor_count + 1
            print(f"✅ Competitor count (FBA within tolerance): {competitor_count} (Total sellers: {total_sellers})")
        except Exception as e:
            print("❌ Failed to extract competitor data:", e)
            total_sellers = 1

        # ------------------------------
        # 15) SALES SIMULATION
        # ------------------------------
        for row in master_rows:
            cp = row["chosen_price"]
            roi_val = row["roi_percent"]
            if roi_val < 0:
                sim_sales = 0
                sim_profit = 0
            else:
                sim_sales = est_daily_sales / total_sellers if total_sellers else 0
                profit_per_unit = cp - break_even
                sim_profit = profit_per_unit * sim_sales

            simulation_results.append({
                "Date": row["date"],
                "Amazon Price": row["amazon_price"],
                "FBA Price": row["fba_price"],
                "FBM Price": row["fbm_price"],
                "Buy Box Price": row["buy_box_price"],  # << ADDED
                "Chosen Price": cp,
                "ROI %": row["roi_percent"],
                "ROI Score": row["roi_score"],
                "Source": row["chosen_source"],
                "BSR": row["bsr"],
                "Offer Count": row["offer_count"],
                "Review Count": row["review_count"],
                "Simulated Sales": sim_sales,
                "Daily Profit": sim_profit,
                "Category": category
            })

        # Summaries
        total_sim_profit = sum(x["Daily Profit"] for x in simulation_results)
        avg_daily_profit = total_sim_profit / len(simulation_results) if simulation_results else 0
        print("\n=== Sales Simulation Projection (Based on 365 Days History) ===")
        print(f"Total projected profit over recorded days: £{total_sim_profit:.2f}")
        print(f"Average daily projected profit: £{avg_daily_profit:.2f}")
        print("\nSimulation Daily Details (last 10 recorded entries):")
        for r in simulation_results[-10:]:
            print(
                f"{r['Date']} (Src: {r['Source']}): Price = £{r['Chosen Price']:.2f}, "
                f"SimSales = {r['Simulated Sales']:.2f}, "
                f"Profit = £{r['Daily Profit']:.2f}, "
                f"BuyBox = {r['Buy Box Price']}, "
                f"Category = {r['Category']}"
            )

    finally:
        # If we created the driver here, quit it
        if created_new_driver:
            driver.quit()

    # ------------------------------
    # 16) RETURN A DATAFRAME
    # ------------------------------
    df = pd.DataFrame(simulation_results)
    return df

# Quick test if run directly
if __name__ == "__main__":
    df_test = run_bbp_chart_analysis("B07C8QSSR2", cost=1.24, vat=20)
    print("\nDataFrame shape:", df_test.shape)
    print(df_test.head(5))
