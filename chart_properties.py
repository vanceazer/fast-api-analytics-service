def get_hour_range(hour):
    to_hour = hour + 1

    def return_string_format(hour):

        if hour > 9:
            return f"{hour}:00"
        else:
            return f"0{hour}:00"

    from_hour_str = return_string_format(hour)
    to_hour_str = return_string_format(to_hour)

    return from_hour_str + "-" + to_hour_str


# Average Violation Analytics
def select_average_violations_query(drill_down, where_clause):
    if drill_down == "day":
        query = f"""
                    SELECT 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE) AS day,
                        COUNT(*) AS avg_violations
                    FROM 
                        dbo.violationschedulerdata
                    {where_clause}
                    GROUP BY 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE)
                    ORDER BY 
                        day;
                """
    elif drill_down == "month":
        query = f"""
                            SELECT year, month, COUNT(*)/30.5 AS avg_violations
                            FROM dbo.violationschedulerdata
                            {where_clause}
                            GROUP BY year, month
                            ORDER BY year,
                            CASE
                                WHEN month = 'January' THEN 1
                                WHEN month = 'February' THEN 2
                                WHEN month = 'March' THEN 3
                                WHEN month = 'April' THEN 4
                                WHEN month = 'May' THEN 5
                                WHEN month = 'June' THEN 6
                                WHEN month = 'July' THEN 7
                                WHEN month = 'August' THEN 8
                                WHEN month = 'September' THEN 9
                                WHEN month = 'October' THEN 10
                                WHEN month = 'November' THEN 11
                                WHEN month = 'December' THEN 12
                            END
                        """
    elif drill_down == "quarter":
        query = f"""
                            SELECT year, quarter, COUNT(*)/92 AS avg_violations
                            FROM dbo.violationschedulerdata
                            {where_clause}
                            GROUP BY year, quarter
                            ORDER BY year, quarter
                        """
    elif drill_down == "year":
        query = f"""
                            SELECT year, COUNT(*)/365 AS avg_violations
                            FROM dbo.violationschedulerdata
                            {where_clause}
                            GROUP BY year
                            ORDER BY year
                        """

    return query

def select_average_violations_fusion_chart_data(drill_down, rows):
    if drill_down == "day":
        fusioncharts_data = {
            "chart": {
                "caption": "Average Violation Per Day By Day",
                "subCaption": "By Day",
                "xAxisName": "Day",
                "yAxisName": "Average Violations",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1"
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            day_str = str(row.day)
            fusioncharts_data["categories"][0]["category"].append({
                "label": day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.avg_violations)
            })

    elif drill_down == "month":
        fusioncharts_data = {
            "chart": {
                "caption": "Average Violation Per Day By Month",
                "subCaption": "By Month",
                "xAxisName": "Month",
                "yAxisName": "Average Violations",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1"
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.month)[:3] + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.avg_violations)
            })

    elif drill_down == "quarter":
        fusioncharts_data = {
            "chart": {
                "caption": "Average Violation Per Day By Quarter",
                "subCaption": "By Quarter",
                "xAxisName": "Quarter",
                "yAxisName": "Average Violations",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1"
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": "Q" + str(row.quarter) + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.avg_violations)
            })

    elif drill_down == "year":
        fusioncharts_data = {
            "chart": {
                "caption": "Average Violation Per Day By Year",
                "subCaption": "By Year",
                "xAxisName": "Year",
                "yAxisName": "Average Violations",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1"
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }


        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.avg_violations)
            })

    return fusioncharts_data

# Total Violation Analytics
def select_total_violations_query(drill_down, where_clause):
    if drill_down == "hour":
        query = f"""
                    SELECT 
                        DATEPART(HOUR, createdat) AS hourpart, 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE) AS day,
                        COUNT(*) AS total_violations
                    FROM 
                        dbo.violationschedulerdata
                    {where_clause}
                    GROUP BY 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE), DATEPART(HOUR, createdat)
                    ORDER BY 
                        day, hourpart;

                """
    elif drill_down == "day":
        query = f"""
                    SELECT 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE) AS day,
                        COUNT(*) AS total_violations
                    FROM 
                        dbo.violationschedulerdata
                    {where_clause}
                    GROUP BY 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE)
                    ORDER BY 
                        day;
                """
    elif drill_down == "month":
        query = f"""
                            SELECT year, month, COUNT(*) AS total_violations
                            FROM dbo.violationschedulerdata
                            {where_clause}
                            GROUP BY year, month
                            ORDER BY year,
                            CASE
                                WHEN month = 'January' THEN 1
                                WHEN month = 'February' THEN 2
                                WHEN month = 'March' THEN 3
                                WHEN month = 'April' THEN 4
                                WHEN month = 'May' THEN 5
                                WHEN month = 'June' THEN 6
                                WHEN month = 'July' THEN 7
                                WHEN month = 'August' THEN 8
                                WHEN month = 'September' THEN 9
                                WHEN month = 'October' THEN 10
                                WHEN month = 'November' THEN 11
                                WHEN month = 'December' THEN 12
                            END
                        """
    elif drill_down == "quarter":
        query = f"""
                            SELECT year, quarter, COUNT(*) AS total_violations
                            FROM dbo.violationschedulerdata
                            {where_clause}
                            GROUP BY year, quarter
                            ORDER BY year, quarter
                        """
    elif drill_down == "year":
        query = f"""
                            SELECT year, COUNT(*) AS total_violations
                            FROM dbo.violationschedulerdata
                            {where_clause}
                            GROUP BY year
                            ORDER BY year
                        """

    return query

def select_total_violations_fusion_chart_data(drill_down, rows):
    if drill_down == "hour":
        fusioncharts_data = {
            "chart": {
                "caption": "Total Violation By Hour",
                "subCaption": "By Hour",
                "xAxisName": "Hour",
                "yAxisName": "Total Violations",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1"
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            from_hour = row.hourpart
            day_str = str(row.day)

            fusioncharts_data["categories"][0]["category"].append({
                "label": get_hour_range(from_hour) + "\n" + day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_violations)
            })

    elif drill_down == "day":
        fusioncharts_data = {
            "chart": {
                "caption": "Total Violation By Day",
                "subCaption": "By Day",
                "xAxisName": "Day",
                "yAxisName": "Total Violations",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1"
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            day_str = str(row.day)
            fusioncharts_data["categories"][0]["category"].append({
                "label": day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_violations)
            })

    elif drill_down == "month":
        fusioncharts_data = {
            "chart": {
                "caption": "Total Violation By Month",
                "subCaption": "By Month",
                "xAxisName": "Month",
                "yAxisName": "Total Violations",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1"
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.month)[:3] + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_violations)
            })

    elif drill_down == "quarter":
        fusioncharts_data = {
            "chart": {
                "caption": "Total Violation By Quarter",
                "subCaption": "By Quarter",
                "xAxisName": "Quarter",
                "yAxisName": "Total Violations",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1"
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": "Q" + str(row.quarter) + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_violations)
            })

    elif drill_down == "year":
        fusioncharts_data = {
            "chart": {
                "caption": "Total Violation By Year",
                "subCaption": "By Year",
                "xAxisName": "Year",
                "yAxisName": "Total Violations",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1"
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }


        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_violations)
            })

    return fusioncharts_data

# Processed Violations Analytics
def select_processed_violations_query(drill_down, where_clause):
    if drill_down == "day":
        query = f"""
                    SELECT 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE) AS day,
                        COUNT(CASE WHEN isprocessed = 'true' THEN 1 END) AS processed_count,
                        COUNT(CASE WHEN isprocessed = 'false' THEN 1 END) AS unprocessed_count,
                        COUNT(*) AS violations_count
                    FROM 
                        dbo.violationschedulerdata
                    {where_clause}
                    GROUP BY 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE)
                    ORDER BY 
                        day;
                        """
    elif drill_down == "month":
        query = f"""
                        SELECT year, month, COUNT(*) as violations_count,
                        COUNT(CASE WHEN isprocessed = 'true' THEN 1 END) AS processed_count,
                        COUNT(CASE WHEN isprocessed = 'false' THEN 1 END) AS unprocessed_count
                        FROM dbo.violationschedulerdata
                        {where_clause}
                        GROUP BY year, month
                        ORDER BY year,
                            CASE
                                WHEN month = 'January' THEN 1
                                WHEN month = 'February' THEN 2
                                WHEN month = 'March' THEN 3
                                WHEN month = 'April' THEN 4
                                WHEN month = 'May' THEN 5
                                WHEN month = 'June' THEN 6
                                WHEN month = 'July' THEN 7
                                WHEN month = 'August' THEN 8
                                WHEN month = 'September' THEN 9
                                WHEN month = 'October' THEN 10
                                WHEN month = 'November' THEN 11
                                WHEN month = 'December' THEN 12
                            END
                    """
    elif drill_down == "quarter":
        query = f"""
                        SELECT year, quarter, COUNT(*) as violations_count,
                        COUNT(CASE WHEN isprocessed = 'true' THEN 1 END) AS processed_count,
                        COUNT(CASE WHEN isprocessed = 'false' THEN 1 END) AS unprocessed_count
                        FROM dbo.violationschedulerdata
                        {where_clause}
                        GROUP BY year, quarter
                        ORDER BY year, quarter
                    """
    elif drill_down == "year":
        query = f"""
                            SELECT year, COUNT(*) as violations_count,
                            COUNT(CASE WHEN isprocessed = 'true' THEN 1 END) AS processed_count,
                            COUNT(CASE WHEN isprocessed = 'false' THEN 1 END) AS unprocessed_count
                            FROM dbo.violationschedulerdata
                            {where_clause}
                            GROUP BY year
                            ORDER BY year
                        """

    return query
def select_processed_violations_fusion_chart_data_combined(drill_down, rows):
    if drill_down == "day":
        fusioncharts_data = {
            "chart": {
                "caption": "Number of Violations by day",
                "xAxisName": "Day",
                "yAxisName": "Violations Count",
                "theme": "fusion",
                "subCaption": "violation comparison by day",
                "numVisiblePlot": "12",
                "scrollheight": "10",
                "flatScrollBars": "1",
                "scrollShowButtons": "0",
                "scrollColor": "#cccccc",
                "showHoverEffect": "1",
                "labelDisplay": "wrap",
                "type": "scrollcombi2d",
                "labelPadding": "20",
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "seriesName": "Total Violations",
                    "showValues": "1",
                    "data": []
                },
                {
                    "seriesName": "Processed",
                    "renderAs": "line",
                    "data": []
                },
                {
                    "seriesName": "Unprocessed",
                    "renderAs": "line",
                    "data": []
                }
            ]
        }

        for row in rows:
            day_str = str(row.day)

            fusioncharts_data["categories"][0]["category"].append({
                "label": day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.violations_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.processed_count)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.unprocessed_count)
            })

    elif drill_down == "month":
        fusioncharts_data = {
            "chart": {
                "caption": "Number of Violations by month",
                "xAxisName": "month",
                "yAxisName": "Violations Count",
                "theme": "fusion",
                "subCaption": "violation comparison by month",
                "numVisiblePlot": "12",
                "scrollheight": "10",
                "flatScrollBars": "1",
                "scrollShowButtons": "0",
                "scrollColor": "#cccccc",
                "showHoverEffect": "1",
                "labelDisplay": "wrap",
                "type": "scrollcombi2d",
                "labelPadding": "20",
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "seriesName": "Total Violations",
                    "showValues": "1",
                    "data": []
                },
                {
                    "seriesName": "Processed",
                    "renderAs": "line",
                    "data": []
                },
                {
                    "seriesName": "Unprocessed",
                    "renderAs": "line",
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.month)[:3] + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.violations_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.processed_count)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.unprocessed_count)
            })

    elif drill_down == "quarter":
        fusioncharts_data = {
            "chart": {
                "caption": "Number of Violations by Quarter",
                "xAxisName": "quarter",
                "yAxisName": "Violations Count",
                "theme": "fusion",
                "subCaption": "violation comparison by quarter",
                "numVisiblePlot": "12",
                "scrollheight": "10",
                "flatScrollBars": "1",
                "scrollShowButtons": "0",
                "scrollColor": "#cccccc",
                "showHoverEffect": "1",
                "labelDisplay": "wrap",
                "type": "scrollcombi2d",
                "labelPadding": "20",
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "seriesName": "Total Violations",
                    "showValues": "1",
                    "data": []
                },
                {
                    "seriesName": "Processed",
                    "renderAs": "line",
                    "data": []
                },
                {
                    "seriesName": "Unprocessed",
                    "renderAs": "line",
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": f"Q{row.quarter}" + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.violations_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.processed_count)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.unprocessed_count)
            })

    elif drill_down == "year":
        fusioncharts_data = {
            "chart": {
                "caption": "Number of Violations by year",
                "xAxisName": "Year",
                "yAxisName": "Violations Count",
                "theme": "fusion",
                "subCaption": "violation comparison by year",
                "divlineColor": "#999999",
                "divLineIsDashed": "1",
                "divLineDashLen": "1",
                "divLineGapLen": "1",
                "toolTipColor": "#ffffff",
                "toolTipBorderThickness": "0",
                "toolTipBgColor": "#000000",
                "toolTipBgAlpha": "80",
                "toolTipBorderRadius": "2",
                "type": "scrollcombi2d",
                "toolTipPadding": "5"
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "seriesName": "Total Violations",
                    "showValues": "1",
                    "data": []
                },
                {
                    "seriesName": "Processed",
                    "renderAs": "line",
                    "data": []
                },
                {
                    "seriesName": "Unprocessed",
                    "renderAs": "line",
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.violations_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.processed_count)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.unprocessed_count)
            })

    return fusioncharts_data
def select_processed_violations_fusion_chart_data_stacked(drill_down, rows):
    if drill_down == "day":
        fusioncharts_data = {
            "chart": {
                "caption": "Number of Violations by day",
                "xAxisName": "Day",
                "yAxisName": "Violations Count",
                "theme": "fusion",
                "subCaption": "violation comparison by day",
                "numVisiblePlot": "12",
                "scrollheight": "10",
                "flatScrollBars": "1",
                "scrollShowButtons": "0",
                "scrollColor": "#cccccc",
                "showHoverEffect": "1",
                "labelDisplay": "wrap",
                "type": "scrollstackedcolumn2d",
                "labelPadding": "20",
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "seriesName": "Processed",
                    "data": []
                },
                {
                    "seriesName": "Unprocessed",
                    "data": []
                }
            ]
        }

        for row in rows:
            day_str = str(row.day)

            fusioncharts_data["categories"][0]["category"].append({
                "label": day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.processed_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.unprocessed_count)
            })

    elif drill_down == "month":
        fusioncharts_data = {
            "chart": {
                "caption": "Number of Violations by month",
                "xAxisName": "month",
                "yAxisName": "Violations Count",
                "theme": "fusion",
                "subCaption": "violation comparison by month",
                "numVisiblePlot": "12",
                "scrollheight": "10",
                "flatScrollBars": "1",
                "scrollShowButtons": "0",
                "scrollColor": "#cccccc",
                "showHoverEffect": "1",
                "labelDisplay": "wrap",
                "type": "scrollstackedcolumn2d",
                "labelPadding": "20",
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "seriesName": "Processed",
                    "data": []
                },
                {
                    "seriesName": "Unprocessed",
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.month)[:3] + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.processed_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.unprocessed_count)
            })

    elif drill_down == "quarter":
        fusioncharts_data = {
            "chart": {
                "caption": "Number of Violations by Quarter",
                "xAxisName": "quarter",
                "yAxisName": "Violations Count",
                "theme": "fusion",
                "subCaption": "violation comparison by quarter",
                "numVisiblePlot": "12",
                "scrollheight": "10",
                "flatScrollBars": "1",
                "scrollShowButtons": "0",
                "scrollColor": "#cccccc",
                "showHoverEffect": "1",
                "labelDisplay": "wrap",
                "type": "scrollstackedcolumn2d",
                "labelPadding": "20",
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "seriesName": "Processed",
                    "data": []
                },
                {
                    "seriesName": "Unprocessed",
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": f"Q{row.quarter}" + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.processed_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.unprocessed_count)
            })

    elif drill_down == "year":
        fusioncharts_data = {
            "chart": {
                "caption": "Number of Violations by year",
                "xAxisName": "Year",
                "yAxisName": "Violations Count",
                "theme": "fusion",
                "subCaption": "violation comparison by year",
                "divlineColor": "#999999",
                "divLineIsDashed": "1",
                "divLineDashLen": "1",
                "divLineGapLen": "1",
                "toolTipColor": "#ffffff",
                "toolTipBorderThickness": "0",
                "toolTipBgColor": "#000000",
                "toolTipBgAlpha": "80",
                "toolTipBorderRadius": "2",
                "type": "scrollstackedcolumn2d",
                "toolTipPadding": "5"
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "seriesName": "Processed",
                    "data": []
                },
                {
                    "seriesName": "Unprocessed",
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.processed_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.unprocessed_count)
            })

    return fusioncharts_data

def select_approval_mode_query(drill_down, where_clause):
    if drill_down == "day":
        query = f"""
                    SELECT 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE) AS day,
                        COUNT(CASE WHEN approvalmode = 1 THEN 1 END) AS pending,
                        COUNT(CASE WHEN approvalmode = 2 THEN 1 END) AS approved,
                        COUNT(CASE WHEN approvalmode = 3 THEN 1 END) AS rejected
                    FROM 
                        dbo.violationschedulerdata
                    {where_clause}
                    GROUP BY 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE)
                    ORDER BY 
                        day;
                        """


    elif drill_down == "month":
        query = f"""
                        SELECT year, month,
                        COUNT(CASE WHEN approvalmode = 1 THEN 1 END) AS pending,
                        COUNT(CASE WHEN approvalmode = 2 THEN 1 END) AS approved,
                        COUNT(CASE WHEN approvalmode = 3 THEN 1 END) AS rejected
                        FROM dbo.violationschedulerdata
                        {where_clause}
                        GROUP BY year, month
                        ORDER BY year,
                            CASE
                                WHEN month = 'January' THEN 1
                                WHEN month = 'February' THEN 2
                                WHEN month = 'March' THEN 3
                                WHEN month = 'April' THEN 4
                                WHEN month = 'May' THEN 5
                                WHEN month = 'June' THEN 6
                                WHEN month = 'July' THEN 7
                                WHEN month = 'August' THEN 8
                                WHEN month = 'September' THEN 9
                                WHEN month = 'October' THEN 10
                                WHEN month = 'November' THEN 11
                                WHEN month = 'December' THEN 12
                            END
                    """

    elif drill_down == "quarter":
        print('processing for quarter')
        query = f"""        
                    SELECT year, quarter,
                    COUNT(CASE WHEN approvalmode = 1 THEN 1 END) AS pending,
                    COUNT(CASE WHEN approvalmode = 2 THEN 1 END) AS approved,
                    COUNT(CASE WHEN approvalmode = 3 THEN 1 END) AS rejected
                    FROM dbo.violationschedulerdata
                    {where_clause}
                    GROUP BY year, quarter
                    ORDER BY year, quarter
                """

    elif drill_down == "year":
        query = f"""
                    SELECT
                        year,
                        COUNT(CASE WHEN approvalmode = 1 THEN 1 END) AS pending,
                        COUNT(CASE WHEN approvalmode = 2 THEN 1 END) AS approved,
                        COUNT(CASE WHEN approvalmode = 3 THEN 1 END) AS rejected
                    FROM
                        dbo.violationschedulerdata
                    {where_clause}
                    GROUP BY year
                    ORDER BY year
                """
    return query



def select_approval_mode_fusion_chart_data_stacked(drill_down, rows):

    fusioncharts_data = {
        "chart": {
            "caption": "Approval Mode Count",
            "xAxisName": drill_down.capitalize(),
            "yAxisName": "Count",
            "theme": "fusion",
            "subCaption": f"Approval Mode comparison by {drill_down}",
            "scrollheight": "10",
            "flatScrollBars": "1",
            "scrollShowButtons": "0",
            "scrollColor": "#cccccc",
            "showHoverEffect": "1",
            "labelDisplay": "wrap",
            "labelPadding": "20",
            "type": "scrollstackedcolumn2d"
        },
        "categories": [
            {
                "category": []
            }
        ],
        "dataset": [
            {
                "seriesName": "Pending",
                "data": []
            },
            {
                "seriesName": "Approved",
                "data": []
            },
            {
                "seriesName": "Rejected",
                "data": []
            }
        ]
    }


    if drill_down == "day":
        for row in rows:
            day_str = str(row.day)

            fusioncharts_data["categories"][0]["category"].append({
                "label": day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.pending)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.approved)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.rejected)
            })

    elif drill_down == "month":
        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.month)[:3] + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.pending)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.approved)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.rejected)
            })

    #
    elif drill_down == "quarter":
        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": f"Q{row.quarter}" + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.pending)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.approved)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.rejected)
            })



    elif drill_down == "year":
        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.pending)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.approved)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.rejected)
            })




    # if not any(d['label'] == label for d in fusioncharts_data["categories"][0]["category"]):
    #     fusioncharts_data["categories"][0]["category"].append({
    #         "label": label
    #     })



    return fusioncharts_data





# Vehicle Count Analytics
def select_vehicle_count_query(drill_down, where_clause):
    if drill_down == "year":
        query = f"""
                    SELECT year, COUNT(*) AS vehicle_count
                    FROM dbo.devicehistory
                    {where_clause}
                    GROUP BY year
                    ORDER BY year ASC
                    """
    elif drill_down == "quarter":
        query = f"""
                    SELECT year, quarter, COUNT(*) AS vehicle_count
                    FROM dbo.devicehistory
                    {where_clause}
                    GROUP BY year, quarter
                    ORDER BY year, quarter 
                    """
    elif drill_down == "month":
        query = f"""
                    SELECT year, month, COUNT(*) AS vehicle_count
                    FROM dbo.devicehistory
                    {where_clause}
                    GROUP BY year, month
                    ORDER BY year,
                        CASE
                            WHEN month = 'January' THEN 1
                            WHEN month = 'February' THEN 2
                            WHEN month = 'March' THEN 3
                            WHEN month = 'April' THEN 4
                            WHEN month = 'May' THEN 5
                            WHEN month = 'June' THEN 6
                            WHEN month = 'July' THEN 7
                            WHEN month = 'August' THEN 8
                            WHEN month = 'September' THEN 9
                            WHEN month = 'October' THEN 10
                            WHEN month = 'November' THEN 11
                            WHEN month = 'December' THEN 12
                        END
                    """
    elif drill_down == "day":
        query = f"""
                    SELECT 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE) AS day,
                        COUNT(*) AS vehicle_count
                    FROM 
                        dbo.devicehistory
                    {where_clause}
                    GROUP BY 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE)
                    ORDER BY 
                        day;
                """
    elif drill_down == "hour":
        query = f"""
                    SELECT 
                        DATEPART(HOUR, createdat) AS hourpart, 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE) AS day,
                        COUNT(*) AS vehicle_count
                    FROM 
                        dbo.devicehistory
                    {where_clause}
                    GROUP BY 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE), DATEPART(HOUR, createdat)
                    ORDER BY 
                        day, hourpart;
                """

    return query

def select_vehicle_count_fusion_chart_data_line(drill_down, rows):
    if drill_down == "year":
        total_vehicle_count = sum(row.vehicle_count for row in rows)
        fusioncharts_data = {
            "chart": {
                "caption": "Vehicle Count by Year",
                "subCaption": "By Year",
                "xAxisName": "Year",
                "yAxisName": "Vehicle Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1",
                "totalVehicleCount": total_vehicle_count
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.vehicle_count)
            })


    elif drill_down == "month":
        total_vehicle_count = sum(row.vehicle_count for row in rows)
        fusioncharts_data = {
            "chart": {
                "caption": "Vehicle Count by Month",
                "subCaption": "By Month",
                "xAxisName": "Month",
                "yAxisName": "Vehicle Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1",
                "totalVehicleCount": total_vehicle_count
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.month)[:3] + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.vehicle_count)
            })
    elif drill_down == "quarter":
        total_vehicle_count = sum(row.vehicle_count for row in rows)
        fusioncharts_data = {
            "chart": {
                "caption": "Vehicle Count by Quarter",
                "subCaption": "By Quarter",
                "xAxisName": "Quarter",
                "yAxisName": "Vehicle Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1",
                "totalVehicleCount": total_vehicle_count
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": "Q" + str(row.quarter) + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.vehicle_count)
            })

    elif drill_down == "day":

        total_vehicle_count = sum(row.vehicle_count for row in rows)

        fusioncharts_data = {
            "chart": {
                "caption": "Vehicle Count by Day",
                "subCaption": "By Day",
                "xAxisName": "Day",
                "yAxisName": "Vehicle Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1",
                "totalVehicleCount": total_vehicle_count
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            day_str = str(row.day)
            fusioncharts_data["categories"][0]["category"].append({
                "label": day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.vehicle_count)
            })

    elif drill_down == "hour":
        total_vehicle_count = sum(row.vehicle_count for row in rows)

        fusioncharts_data = {
            "chart": {
                "caption": "Vehicle Count by Hour",
                "subCaption": "By Hour",
                "xAxisName": "Hour",
                "yAxisName": "Vehicle Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollline2d",
                "showHoverEffect": "1",
                "totalVehicleCount": total_vehicle_count
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            from_hour = row.hourpart
            day_str = str(row.day)

            fusioncharts_data["categories"][0]["category"].append({
                "label": get_hour_range(from_hour) + "\n" + day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.vehicle_count)
            })

    return fusioncharts_data

def select_vehicle_count_fusion_chart_data_column(drill_down, rows):
    if drill_down == "year":
        total_vehicle_count = sum(row.vehicle_count for row in rows)
        fusioncharts_data = {
            "chart": {
                "caption": "Vehicle Count by Year",
                "subCaption": "By Year",
                "xAxisName": "Year",
                "yAxisName": "Vehicle Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollcolumn2d",
                "showHoverEffect": "1",
                "totalVehicleCount": total_vehicle_count
            },
            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.vehicle_count)
            })


    elif drill_down == "month":
        total_vehicle_count = sum(row.vehicle_count for row in rows)
        fusioncharts_data = {
            "chart": {
                "caption": "Vehicle Count by Month",
                "subCaption": "By Month",
                "xAxisName": "Month",
                "yAxisName": "Vehicle Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollcolumn2d",
                "showHoverEffect": "1",
                "totalVehicleCount": total_vehicle_count
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.month)[:3] + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.vehicle_count)
            })
    elif drill_down == "quarter":
        total_vehicle_count = sum(row.vehicle_count for row in rows)
        fusioncharts_data = {
            "chart": {
                "caption": "Vehicle Count by Quarter",
                "subCaption": "By Quarter",
                "xAxisName": "Quarter",
                "yAxisName": "Vehicle Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollcolumn2d",
                "showHoverEffect": "1",
                "totalVehicleCount": total_vehicle_count
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": "Q" + str(row.quarter) + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.vehicle_count)
            })

    elif drill_down == "day":

        total_vehicle_count = sum(row.vehicle_count for row in rows)

        fusioncharts_data = {
            "chart": {
                "caption": "Vehicle Count by Day",
                "subCaption": "By Day",
                "xAxisName": "Day",
                "yAxisName": "Vehicle Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollcolumn2d",
                "showHoverEffect": "1",
                "totalVehicleCount": total_vehicle_count
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            day_str = str(row.day)
            fusioncharts_data["categories"][0]["category"].append({
                "label": day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.vehicle_count)
            })

    elif drill_down == "hour":
        total_vehicle_count = sum(row.vehicle_count for row in rows)

        fusioncharts_data = {
            "chart": {
                "caption": "Vehicle Count by Hour",
                "subCaption": "By Hour",
                "xAxisName": "Hour",
                "yAxisName": "Vehicle Count",
                "theme": "fusion",
                "lineThickness": "3",
                "flatScrollBars": "1",
                "scrollheight": "10",
                "numVisiblePlot": "12",
                "type": "scrollcolumn2d",
                "showHoverEffect": "1",
                "totalVehicleCount": total_vehicle_count
            },

            "categories": [
                {
                    "category": []
                }
            ],
            "dataset": [
                {
                    "data": []
                }
            ]
        }

        for row in rows:
            from_hour = row.hourpart
            day_str = str(row.day)

            fusioncharts_data["categories"][0]["category"].append({
                "label": get_hour_range(from_hour) + "\n" + day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.vehicle_count)
            })

    return fusioncharts_data



def get_fusion_chart_template(x_axis):
    return {
        "chart": {
            "caption": "Number of Watchlist Hits",
            "xAxisName": x_axis,
            "yAxisName": "Watchlist Hits",
            "theme": "fusion",
            "lineThickness": "3",
            "flatScrollBars": "1",
            "scrollheight": "10",
            "numVisiblePlot": "12",
            "type": "scrollColumn2D",
            "showHoverEffect": "1"
        },
        "categories": [
            {
                "category": []
            }
        ],
        "dataset": [
            {
                "data": []
            }
        ]
    }

def format_fusion_chart_data(labels, counts):
    categories = [{"label": label} for label in labels]
    dataset = [{"value": count} for count in counts]
    return categories, dataset


def select_total_watchlist_hits(drill_down, where_clause):
    if drill_down == "hour":
        query = f"""
                    SELECT 
                        DATEPART(HOUR, createdat) AS hourpart, 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE) AS day,
                        COUNT(*) AS total_hits
                    FROM 
                        dbo.watchlisthits
                    {where_clause}
                    GROUP BY 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE), DATEPART(HOUR, createdat)
                    ORDER BY 
                        day, hourpart;

                """
    elif drill_down == "day":
        query = f"""
                    SELECT 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE) AS day,
                        COUNT(*) AS total_hits
                    FROM 
                        dbo.watchlisthits
                    {where_clause}
                    GROUP BY 
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE)
                    ORDER BY 
                        day;
                """
    elif drill_down == "month":
        query = f"""
                            SELECT year, month, COUNT(*) AS total_hits
                            FROM dbo.watchlisthits
                            {where_clause}
                            GROUP BY year, month
                            ORDER BY year,
                            CASE
                                WHEN month = 'January' THEN 1
                                WHEN month = 'February' THEN 2
                                WHEN month = 'March' THEN 3
                                WHEN month = 'April' THEN 4
                                WHEN month = 'May' THEN 5
                                WHEN month = 'June' THEN 6
                                WHEN month = 'July' THEN 7
                                WHEN month = 'August' THEN 8
                                WHEN month = 'September' THEN 9
                                WHEN month = 'October' THEN 10
                                WHEN month = 'November' THEN 11
                                WHEN month = 'December' THEN 12
                            END
                        """
    elif drill_down == "quarter":
        query = f"""
                            SELECT year, quarter, COUNT(*) AS total_hits
                            FROM dbo.watchlisthits
                            {where_clause}
                            GROUP BY year, quarter
                            ORDER BY year, quarter
                        """
    elif drill_down == "year":
        query = f"""
                            SELECT year, COUNT(*) AS total_hits
                            FROM dbo.watchlisthits
                            {where_clause}
                            GROUP BY year
                            ORDER BY year
                        """

    return query

def select_watchlist_hits_fusion_chart(chartype, drill_down, rows):
    fusioncharts_data = {
        "chart": {
            "caption": "Number of Watchlist Hits",
            "subCaption": f"Number of Hits by {drill_down}",
            "xAxisName": drill_down.capitalize(),
            "yAxisName": "Total Violations",
            "theme": "fusion",
            "lineThickness": "3",
            "flatScrollBars": "1",
            "scrollheight": "10",
            "numVisiblePlot": "12",
            "type": chartype,
            "showHoverEffect": "1"
        },

        "categories": [
            {
                "category": []
            }
        ],
        "dataset": [
            {
                "data": []
            }
        ]
    }

    if drill_down == "hour":
        for row in rows:
            from_hour = row.hourpart
            day_str = str(row.day)

            fusioncharts_data["categories"][0]["category"].append({
                "label": get_hour_range(from_hour) + "\n" + day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_hits)
            })


    elif drill_down == "day":
        for row in rows:
            day_str = str(row.day)
            fusioncharts_data["categories"][0]["category"].append({
                "label": day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_hits)
            })

    elif drill_down == "month":
        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.month)[:3] + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_hits)
            })



    elif drill_down == "quarter":
        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": "Q" + str(row.quarter) + "\n" + str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_hits)
            })




    elif drill_down == "year":
        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_hits)
            })






    return fusioncharts_data




# # Paid Offence Analytics
def select_offence_query(drill_down, where_clause):
    if drill_down == "day":
        query = f"""
                    SELECT
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE) AS day,
                        COUNT(CASE WHEN paymentstatusname = 'Paid' THEN 1 END) AS paid_count,
                        COUNT(CASE WHEN paymentstatusname = 'Not Paid' THEN 1 END) AS unpaid_count,
                        COUNT(*) AS paymentstatus_count
                    FROM
                        dbo.offencepaymentdata
                    {where_clause}
                    GROUP BY
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE)
                    ORDER BY
                        day;
                        """
    elif drill_down == "month":
        query = f"""
                    SELECT createdat_year, createdat_month, COUNT(*) as violations_count,
                    COUNT(CASE WHEN paymentstatusname = 'Paid' THEN 1 END) AS paid_count,
                    COUNT(CASE WHEN paymentstatusname = 'Not Paid' THEN 1 END) AS unpaid_count,
                    COUNT(*) AS paymentstatus_count
                    FROM dbo.offencepaymentdata
                    {where_clause}
                    GROUP BY createdat_year, createdat_month
                    ORDER BY createdat_year,
                        CASE
                            WHEN createdat_month = 'January' THEN 1
                            WHEN createdat_month = 'February' THEN 2
                            WHEN createdat_month = 'March' THEN 3
                            WHEN createdat_month = 'April' THEN 4
                            WHEN createdat_month = 'May' THEN 5
                            WHEN createdat_month = 'June' THEN 6
                            WHEN createdat_month = 'July' THEN 7
                            WHEN createdat_month = 'August' THEN 8
                            WHEN createdat_month = 'September' THEN 9
                            WHEN createdat_month = 'October' THEN 10
                            WHEN createdat_month = 'November' THEN 11
                            WHEN createdat_month = 'December' THEN 12
                        END
                    """
    elif drill_down == "quarter":
        query = f"""
                    SELECT createdat_year, createdat_quarter, COUNT(*) as violations_count,
                    COUNT(CASE WHEN paymentstatusname = 'Paid' THEN 1 END) AS paid_count,
                    COUNT(CASE WHEN paymentstatusname = 'Not Paid' THEN 1 END) AS unpaid_count,
                    COUNT(*) AS paymentstatus_count
                    FROM dbo.offencepaymentdata
                    {where_clause}
                    GROUP BY createdat_year, createdat_quarter
                    ORDER BY createdat_year, createdat_quarter
                    """
    elif drill_down == "year":
        query = f"""
                    SELECT createdat_year, COUNT(*) as violations_count,
                    COUNT(CASE WHEN paymentstatusname = 'Paid' THEN 1 END) AS paid_count,
                    COUNT(CASE WHEN paymentstatusname = 'Not Paid' THEN 1 END) AS unpaid_count,
                    COUNT(*) AS paymentstatus_count
                    FROM dbo.offencepaymentdata
                    {where_clause}
                    GROUP BY createdat_year
                    ORDER BY createdat_year
                """

    return query
def select_offence_fusion_chart_data_combined(drill_down, rows):

    fusioncharts_data = {
        "chart": {
            "caption": "Number of Vehicle Offence",
            "xAxisName": drill_down.capitalize(),
            "yAxisName": "Count",
            "theme": "fusion",
            "subCaption": f"Number of Vehicle Offences by {drill_down}",
            "numVisiblePlot": "12",
            "scrollheight": "10",
            "flatScrollBars": "1",
            "scrollShowButtons": "0",
            "scrollColor": "#cccccc",
            "showHoverEffect": "1",
            "labelDisplay": "wrap",
            "type": "scrollcombi2d",
            "labelPadding": "20",
        },
        "categories": [
            {
                "category": []
            }
        ],
        "dataset": [
            {
                "seriesName": "Total Offence Payments",
                "showValues": "1",
                "data": []
            },
            {
                "seriesName": "Paid",
                "renderAs": "line",
                "data": []
            },
            {
                "seriesName": "Not Paid",
                "renderAs": "line",
                "data": []
            }
        ]
    }

    if drill_down == "day":

        for row in rows:
            day_str = str(row.day)

            fusioncharts_data["categories"][0]["category"].append({
                "label": day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.paymentstatus_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.paid_count)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.unpaid_count)
            })

    elif drill_down == "month":

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.createdat_month)[:3] + "\n" + str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.paymentstatus_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.paid_count)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.unpaid_count)
            })

    elif drill_down == "quarter":

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": f"Q{row.createdat_quarter}" + "\n" + str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.paymentstatus_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.paid_count)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.unpaid_count)
            })

    elif drill_down == "year":

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.paymentstatus_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.paid_count)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.unpaid_count)
            })

    return fusioncharts_data



def select_offence_fusion_chart_data_stacked(drill_down, rows):

    fusioncharts_data = {
        "chart": {
            "caption": "Number of Vehicle Offence",
            "xAxisName": drill_down.capitalize(),
            "yAxisName": "Count",
            "theme": "fusion",
            "subCaption": f"Number of Vehicle Offences by {drill_down}",
            "numVisiblePlot": "12",
            "scrollheight": "10",
            "flatScrollBars": "1",
            "scrollShowButtons": "0",
            "scrollColor": "#cccccc",
            "showHoverEffect": "1",
            "labelDisplay": "wrap",
            "type": "scrollstackedcolumn2d",
            "labelPadding": "20",
        },
        "categories": [
            {
                "category": []
            }
        ],
        "dataset": [
            {
                "seriesName": "Paid",
                "data": []
            },
            {
                "seriesName": "Not Paid",
                "data": []
            }
        ]
    }

    if drill_down == "day":
        for row in rows:
            day_str = str(row.day)

            fusioncharts_data["categories"][0]["category"].append({
                "label": day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.paid_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.unpaid_count)
            })

    elif drill_down == "month":

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.createdat_month)[:3] + "\n" + str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.paid_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.unpaid_count)
            })

    elif drill_down == "quarter":

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": f"Q{row.createdat_quarter}" + "\n" + str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.paid_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.unpaid_count)
            })

    elif drill_down == "year":

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.paid_count)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.unpaid_count)
            })

    return fusioncharts_data

# Received and Receivables Analytics
def select_offence_amount_query(drill_down, where_clause):
    if drill_down == "day":
        query = f"""
                    SELECT
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE) AS day,
                        SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount END) AS paid_amount,
                        SUM(CASE WHEN paymentstatusname = 'Not Paid' THEN fineamount END) AS unpaid_amount,
                        SUM(fineamount) AS total_amount
                    FROM
                        dbo.offencepaymentdata
                    {where_clause}
                    GROUP BY
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE)
                    ORDER BY
                        day;
                        """
    elif drill_down == "month":
        query = f"""
                    SELECT createdat_year, createdat_month,
                        SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount END) AS paid_amount,
                        SUM(CASE WHEN paymentstatusname = 'Not Paid' THEN fineamount END) AS unpaid_amount,
                        SUM(fineamount) AS total_amount
                    FROM dbo.offencepaymentdata
                    {where_clause}
                    GROUP BY createdat_year, createdat_month
                    ORDER BY createdat_year,
                        CASE
                            WHEN createdat_month = 'January' THEN 1
                            WHEN createdat_month = 'February' THEN 2
                            WHEN createdat_month = 'March' THEN 3
                            WHEN createdat_month = 'April' THEN 4
                            WHEN createdat_month = 'May' THEN 5
                            WHEN createdat_month = 'June' THEN 6
                            WHEN createdat_month = 'July' THEN 7
                            WHEN createdat_month = 'August' THEN 8
                            WHEN createdat_month = 'September' THEN 9
                            WHEN createdat_month = 'October' THEN 10
                            WHEN createdat_month = 'November' THEN 11
                            WHEN createdat_month = 'December' THEN 12
                        END
                    """
    elif drill_down == "quarter":
        query = f"""
                    SELECT createdat_year, createdat_quarter,
                        SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount END) AS paid_amount,
                        SUM(CASE WHEN paymentstatusname = 'Not Paid' THEN fineamount END) AS unpaid_amount,
                        SUM(fineamount) AS total_amount
                    FROM dbo.offencepaymentdata
                    {where_clause}
                    GROUP BY createdat_year, createdat_quarter
                    ORDER BY createdat_year, createdat_quarter
                    """
    elif drill_down == "year":
        query = f"""
                    SELECT createdat_year,
                        SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount END) AS paid_amount,
                        SUM(CASE WHEN paymentstatusname = 'Not Paid' THEN fineamount END) AS unpaid_amount,
                        SUM(fineamount) AS total_amount
                    FROM dbo.offencepaymentdata
                    {where_clause}
                    GROUP BY createdat_year
                    ORDER BY createdat_year
                """

    return query
def select_offence_amount_fusion_chart_data_combined(drill_down, rows):
    fusioncharts_data = {
        "chart": {
            "caption": "Value Generated From Fines",
            "xAxisName": drill_down.capitalize(),
            "yAxisName": "Amount",
            "theme": "fusion",
            "subCaption": f"Fines Value Generated by {drill_down}",
            "numVisiblePlot": "12",
            "scrollheight": "10",
            "flatScrollBars": "1",
            "scrollShowButtons": "0",
            "scrollColor": "#cccccc",
            "showHoverEffect": "1",
            "labelDisplay": "wrap",
            "type": "scrollcombi2d",
            "labelPadding": "20",
        },
        "categories": [
            {
                "category": []
            }
        ],
        "dataset": [
            {
                "seriesName": "Total Fine Amount",
                "showValues": "1",
                "data": []
            },
            {
                "seriesName": "Fines Received",
                "renderAs": "line",
                "data": []
            },
            {
                "seriesName": "Receivables",
                "renderAs": "line",
                "data": []
            }
        ]
    }
    if drill_down == "day":
        for row in rows:
            day_str = str(row.day)

            fusioncharts_data["categories"][0]["category"].append({
                "label": day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_amount)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.paid_amount)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.unpaid_amount)
            })

    elif drill_down == "month":


        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.createdat_month)[:3] + "\n" + str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_amount)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.paid_amount)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.unpaid_amount)
            })

    elif drill_down == "quarter":

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": f"Q{row.createdat_quarter}" + "\n" + str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_amount)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.paid_amount)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.unpaid_amount)
            })

    elif drill_down == "year":

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.total_amount)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.paid_amount)
            })

            fusioncharts_data["dataset"][2]["data"].append({
                "value": str(row.unpaid_amount)
            })

    return fusioncharts_data
def select_offence_amount_fusion_chart_data_stacked(drill_down, rows):

    fusioncharts_data = {
        "chart": {
            "caption": "Value Generated From Fines",
            "xAxisName": drill_down.capitalize(),
            "yAxisName": "Amount",
            "theme": "fusion",
            "subCaption": f"Fines Value Generated by {drill_down}",
            "numVisiblePlot": "12",
            "scrollheight": "10",
            "flatScrollBars": "1",
            "scrollShowButtons": "0",
            "scrollColor": "#cccccc",
            "showHoverEffect": "1",
            "labelDisplay": "wrap",
            "type": "scrollstackedcolumn2d",
            "labelPadding": "20",
        },
        "categories": [
            {
                "category": []
            }
        ],
        "dataset": [
            {
                "seriesName": "Fines Received",
                "data": []
            },
            {
                "seriesName": "Receivables",
                "data": []
            }
        ]
    }
    if drill_down == "day":

        for row in rows:
            day_str = str(row.day)

            fusioncharts_data["categories"][0]["category"].append({
                "label": day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.paid_amount)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.unpaid_amount)
            })

    elif drill_down == "month":

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.createdat_month)[:3] + "\n" + str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.paid_amount)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.unpaid_amount)
            })

    elif drill_down == "quarter":

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": f"Q{row.createdat_quarter}" + "\n" + str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.paid_amount)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.unpaid_amount)
            })

    elif drill_down == "year":

        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": str(row.paid_amount)
            })

            fusioncharts_data["dataset"][1]["data"].append({
                "value": str(row.unpaid_amount)
            })

    return fusioncharts_data


def select_conversion_rate_query(drill_down, where_clause):
    if drill_down == "day":
        query = f"""
                    SELECT
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE) AS day,
                        SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount END) AS paid_amount,
                        ROUND((SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount ELSE 0 END) / SUM(fineamount)) * 100, 2) AS conversion_rate
                        
                    FROM
                        dbo.offencepaymentdata
                    {where_clause}
                    GROUP BY
                        CAST(CONVERT(DATETIME, createdat, 126) AS DATE)
                    ORDER BY
                        day;
                        """
    elif drill_down == "month":
        query = f"""
                    SELECT createdat_year, createdat_month,
                        SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount END) AS paid_amount,
                        ROUND((SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount ELSE 0 END) / SUM(fineamount)) * 100, 2) AS conversion_rate
                    FROM dbo.offencepaymentdata
                    {where_clause}
                    GROUP BY createdat_year, createdat_month
                    ORDER BY createdat_year,
                        CASE
                            WHEN createdat_month = 'January' THEN 1
                            WHEN createdat_month = 'February' THEN 2
                            WHEN createdat_month = 'March' THEN 3
                            WHEN createdat_month = 'April' THEN 4
                            WHEN createdat_month = 'May' THEN 5
                            WHEN createdat_month = 'June' THEN 6
                            WHEN createdat_month = 'July' THEN 7
                            WHEN createdat_month = 'August' THEN 8
                            WHEN createdat_month = 'September' THEN 9
                            WHEN createdat_month = 'October' THEN 10
                            WHEN createdat_month = 'November' THEN 11
                            WHEN createdat_month = 'December' THEN 12
                        END
                    """
    elif drill_down == "quarter":
        query = f"""
                    SELECT createdat_year, createdat_quarter,
                        SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount END) AS paid_amount,
                        ROUND((SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount ELSE 0 END) / SUM(fineamount)) * 100, 2) AS conversion_rate
                    FROM dbo.offencepaymentdata
                    {where_clause}
                    GROUP BY createdat_year, createdat_quarter
                    ORDER BY createdat_year, createdat_quarter
                    """
    elif drill_down == "year":
        query = f"""
                    SELECT createdat_year,
                        SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount END) AS paid_amount,
                        ROUND((SUM(CASE WHEN paymentstatusname = 'Paid' THEN fineamount ELSE 0 END) / SUM(fineamount)) * 100, 2) AS conversion_rate
                    FROM dbo.offencepaymentdata
                    {where_clause}
                    GROUP BY createdat_year
                    ORDER BY createdat_year
                """

    return query


def select_conversion_rate_fusion_chart(chartype, drill_down, rows):
    fusioncharts_data = {
        "chart": {
            "caption": "Vehicle Offence Payment Conversion Rate",
            "subCaption": f"Conversion Rate by {drill_down}",
            "xAxisName": drill_down.capitalize(),
            "yAxisName": "Conversion Rate in Percentage",
            "theme": "fusion",
            "lineThickness": "3",
            "flatScrollBars": "1",
            "scrollheight": "10",
            "numVisiblePlot": "12",
            "type": chartype,
            "showHoverEffect": "1"
        },

        "categories": [
            {
                "category": []
            }
        ],
        "dataset": [
            {
                "data": []
            }
        ]
    }


    if drill_down == "day":
        for row in rows:
            day_str = str(row.day)
            fusioncharts_data["categories"][0]["category"].append({
                "label": day_str[8:] + "/" + day_str[5:7] + "\n" + day_str[:4]
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": f"{row.conversion_rate:.2f}"
            })

    elif drill_down == "month":
        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.createdat_month)[:3] + "\n" + str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": f"{row.conversion_rate:.2f}"
            })



    elif drill_down == "quarter":
        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": "Q" + str(row.createdat_quarter) + "\n" + str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": f"{row.conversion_rate:.2f}"
            })




    elif drill_down == "year":
        for row in rows:
            fusioncharts_data["categories"][0]["category"].append({
                "label": str(row.createdat_year)
            })

            fusioncharts_data["dataset"][0]["data"].append({
                "value": f"{row.conversion_rate:.2f}"
            })


    return fusioncharts_data


