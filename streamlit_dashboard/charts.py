import altair as alt
import pandas as pd


NEUTRAL = "#1F4E5F"
OK = "#2E8B57"
WARN = "#C97A1A"
ALERT = "#C44536"
ACCENT = "#0F766E"


def _base_chart(data: pd.DataFrame, title: str | None = None) -> alt.Chart:
    if title is None:
        return alt.Chart(data)
    return alt.Chart(data, title=title)


def bar_chart(
    data: pd.DataFrame,
    x: str,
    y: str,
    title: str | None = None,
    color: str = ACCENT,
    horizontal: bool = False,
    sort: str | list[str] = "-x",
    height: int = 320,
    x_title: str | None = None,
    y_title: str | None = None,
    x_label_angle: int | None = None,
) -> alt.Chart:
    chart = _base_chart(data, title=title)
    if horizontal:
        encoded = chart.mark_bar(size=18, cornerRadiusEnd=5, color=color).encode(
            x=alt.X(f"{x}:Q", title=x_title),
            y=alt.Y(f"{y}:N", sort=sort, title=y_title),
            tooltip=list(data.columns),
        )
    else:
        encoded = chart.mark_bar(size=28, cornerRadiusTopLeft=5, cornerRadiusTopRight=5, color=color).encode(
            x=alt.X(
                f"{x}:N",
                sort=sort,
                title=x_title,
                axis=alt.Axis(labelAngle=x_label_angle),
            ),
            y=alt.Y(f"{y}:Q", title=y_title),
            tooltip=list(data.columns),
        )
    return (
        encoded.properties(height=height)
        .configure(background="#F6EEDF")
        .configure_view(strokeWidth=0)
        .configure_axis(
            labelColor="#38505B",
            titleColor="#38505B",
            gridColor="#E3E7E0",
            tickColor="#E3E7E0",
        )
        .configure_title(
            anchor="start",
            color="#17313B",
            fontSize=16,
            fontWeight="bold",
        )
    )


def line_chart(
    data: pd.DataFrame,
    x: str,
    y: str,
    title: str | None = None,
    color: str = NEUTRAL,
    height: int = 320,
    x_title: str | None = None,
    y_title: str | None = None,
) -> alt.Chart:
    encoded = (
        _base_chart(data, title=title)
        .mark_line(point=alt.OverlayMarkDef(size=64, filled=True), strokeWidth=3, color=color)
        .encode(
            x=alt.X(f"{x}:T", title=x_title),
            y=alt.Y(f"{y}:Q", title=y_title),
            tooltip=list(data.columns),
        )
    )
    return (
        encoded.properties(height=height)
        .configure(background="#F6EEDF")
        .configure_view(strokeWidth=0)
        .configure_axis(
            labelColor="#38505B",
            titleColor="#38505B",
            gridColor="#E3E7E0",
            tickColor="#E3E7E0",
        )
        .configure_title(
            anchor="start",
            color="#17313B",
            fontSize=16,
            fontWeight="bold",
        )
    )


def area_chart(
    data: pd.DataFrame,
    x: str,
    y: str,
    title: str | None = None,
    color: str = WARN,
    height: int = 320,
    x_title: str | None = None,
    y_title: str | None = None,
) -> alt.Chart:
    encoded = (
        _base_chart(data, title=title)
        .mark_area(color=color, opacity=0.28, line={"color": color, "strokeWidth": 2})
        .encode(
            x=alt.X(f"{x}:T", title=x_title),
            y=alt.Y(f"{y}:Q", title=y_title),
            tooltip=list(data.columns),
        )
    )
    return (
        encoded.properties(height=height)
        .configure(background="#F6EEDF")
        .configure_view(strokeWidth=0)
        .configure_axis(
            labelColor="#38505B",
            titleColor="#38505B",
            gridColor="#E3E7E0",
            tickColor="#E3E7E0",
        )
        .configure_title(
            anchor="start",
            color="#17313B",
            fontSize=16,
            fontWeight="bold",
        )
    )
