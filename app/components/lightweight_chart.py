from __future__ import annotations

import json

import streamlit.components.v1 as components


def render_method_comparison_chart(
    *,
    lightweight_charts_js: str,
    method_0_data: list[dict],
    method_1_data: list[dict],
    diff_pct_data: list[dict],
    abs_diff_pct_data: list[dict],
) -> None:
    method_0_json = json.dumps(method_0_data)
    method_1_json = json.dumps(method_1_data)
    diff_pct_json = json.dumps(diff_pct_data)
    abs_diff_pct_json = json.dumps(abs_diff_pct_data)

    html = f"""
    <div id="price-chart" style="height: 480px; width: 100%;"></div>
    <div id="diff-chart" style="height: 260px; width: 100%; margin-top: 12px;"></div>

    <script>
    {lightweight_charts_js}
    </script>

    <script>
    const method0Data = {method_0_json};
    const method1Data = {method_1_json};
    const diffPctData = {diff_pct_json};
    const absDiffPctData = {abs_diff_pct_json};

    const priceElement = document.getElementById("price-chart");
    const diffElement = document.getElementById("diff-chart");

    const commonOptions = {{
        layout: {{
            background: {{ color: "#ffffff" }},
            textColor: "#222222",
        }},
        grid: {{
            vertLines: {{ color: "#eeeeee" }},
            horzLines: {{ color: "#eeeeee" }},
        }},
        localization: {{
            timeFormatter: (time) => {{
                const date = new Date(time * 1000);
                return date.toISOString().slice(0, 16).replace("T", " ");
            }},
        }},
        timeScale: {{
            timeVisible: true,
            secondsVisible: false,
            tickMarkFormatter: (time) => {{
                const date = new Date(time * 1000);
                return date.toISOString().slice(5, 16).replace("T", " ");
            }},
        }},
    }};

    const priceChart = LightweightCharts.createChart(priceElement, {{
        ...commonOptions,
        width: priceElement.clientWidth,
        height: 480,
        rightPriceScale: {{
            borderVisible: false,
        }},
    }});

    const method0Series = priceChart.addLineSeries({{
        title: "method 0 close",
        color: "#2962ff",
        lineWidth: 2,
    }});

    method0Series.setData(method0Data);

    const method1Series = priceChart.addLineSeries({{
        title: "method 1 close",
        color: "#111111",
        lineWidth: 2,
    }});

    method1Series.setData(method1Data);

    const diffChart = LightweightCharts.createChart(diffElement, {{
        ...commonOptions,
        width: diffElement.clientWidth,
        height: 260,
        rightPriceScale: {{
            borderVisible: false,
        }},
    }});

    const diffSeries = diffChart.addHistogramSeries({{
        title: "method 1 - method 0 (%)",
        priceFormat: {{
            type: "price",
            precision: 4,
            minMove: 0.0001,
        }},
    }});

    diffSeries.setData(diffPctData);

    const absDiffSeries = diffChart.addLineSeries({{
        title: "abs(method 1 - method 0) (%)",
        color: "#ff9800",
        lineWidth: 2,
        priceFormat: {{
            type: "price",
            precision: 4,
            minMove: 0.0001,
        }},
    }});

    absDiffSeries.setData(absDiffPctData);

    priceChart.timeScale().fitContent();
    diffChart.timeScale().fitContent();

    priceChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {{
        diffChart.timeScale().setVisibleLogicalRange(range);
    }});

    diffChart.timeScale().subscribeVisibleLogicalRangeChange((range) => {{
        priceChart.timeScale().setVisibleLogicalRange(range);
    }});

    window.addEventListener("resize", () => {{
        priceChart.applyOptions({{
            width: priceElement.clientWidth,
        }});
        diffChart.applyOptions({{
            width: diffElement.clientWidth,
        }});
    }});
    </script>
    """

    components.html(
        html,
        height=800,
    )