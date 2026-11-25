/* =========================================================================
   区域空气质量监测与污染特征分析 · 大屏脚本
   数据全部来自 ../output/*.json，JS 中不写死任何数值、结论。
   国标标识色由 level_distribution.json / calendar.json 的 meta 提供，
   仅在显示层对六级高饱和国标色统一做柔化（只调明度/饱和度，不改色相），不改动数据本身的国标色。
   ========================================================================= */

const SRC = '../output/';
const FILES = ['kpi', 'daily_series', 'level_distribution', 'primary_pollutant',
  'station_rank', 'weather_effect', 'calendar', 'monthly_compare', 'conclusions'];

const charts = {};
let LEVELS = [];
let tickerItems = [];
let tickerIdx = 0;

const AXIS = '#6b8296';
const GRID_LINE = 'rgba(91,155,213,.14)';
const TOOLTIP = {
  backgroundColor: 'rgba(255,255,255,.96)',
  borderColor: 'rgba(91,155,213,.35)',
  borderWidth: 1,
  textStyle: { color: '#33475b', fontSize: 12 },
  extraCssText: 'box-shadow:0 6px 24px rgba(80,110,140,.28);border-radius:3px;'
};

const $ = id => document.getElementById(id);
const fmt = (v, n = 1) => (v === null || v === undefined || isNaN(v)) ? '-' : Number(v).toFixed(n);

function hexA(hex, a) {
  const h = hex.replace('#', '');
  const r = parseInt(h.slice(0, 2), 16), g = parseInt(h.slice(2, 4), 16), b = parseInt(h.slice(4, 6), 16);
  return `rgba(${r},${g},${b},${a})`;
}
const SOFTEN = {
  '#00E400': '#7BC98B',
  '#FFFF00': '#F7E8A0',
  '#FF7E00': '#F5B877',
  '#FF0000': '#E88A86',
  '#99004C': '#C486A8',
  '#7E0023': '#A87380'
};
function soften(hex) { return SOFTEN[hex] || hex; }

function levelColor(aqi) {
  for (const L of LEVELS) if (aqi >= L.lo && aqi <= L.hi) return soften(L.color);
  return '#b0bec5';
}
function levelName(aqi) {
  for (const L of LEVELS) if (aqi >= L.lo && aqi <= L.hi) return L.level;
  return '未知';
}

async function getJSON(name) {
  const res = await fetch(SRC + name + '.json', { cache: 'no-store' });
  if (!res.ok) throw new Error(name + '.json → HTTP ' + res.status);
  return res.json();
}

function baseChart(id) {
  if (!charts[id]) charts[id] = echarts.init($(id), null, { renderer: 'canvas' });
  return charts[id];
}

async function loadAll(first) {
  const [kpi, daily, level, primary, rank, weather, cal, month, concl] =
    await Promise.all(FILES.map(getJSON));

  LEVELS = level.data.map(d => ({ level: d.level, lo: d.lo, hi: d.hi, color: d.color }));

  renderHeader(kpi, cal);
  renderKpi(kpi);
  renderTrend(daily);
  renderLevel(level, kpi);
  renderPrimary(primary);
  renderRank(rank);
  renderScatter(weather);
  renderMonth(month);
  renderCalendar(cal);

  tickerItems = concl.data.items;
  if (first) startTicker();
  $('refresh-time').textContent = new Date().toLocaleTimeString('zh-CN', { hour12: false });
}

function renderHeader(kpi, cal) {
  const k = kpi.data;
  $('hd-sub').textContent =
    `${kpi.meta.period} · ${k.stationCount} 个站点 · ${k.countyCount} 个区县 · ` +
    `${k.recordCount} 条逐时记录 · ${k.validDays} 个有效天`;
}

function startTicker() {
  const el = $('ticker-text');
  if (!tickerItems.length) return;
  const show = () => {
    el.textContent = tickerItems[tickerIdx % tickerItems.length];
    tickerIdx++;
  };
  show();
  setInterval(() => {
    el.classList.add('fade');
    setTimeout(() => { show(); el.classList.remove('fade'); }, 420);
  }, 6000);
}

function renderKpi(kpi) {
  const k = kpi.data;
  $('k-goodrate').textContent = fmt(k.goodRate, 2);
  $('k-gooddays').textContent = `优良 ${k.goodDays} / 有效 ${k.validDays} 天`;
  $('k-heavy').textContent = k.heavyDays;
  $('k-pm25').textContent = fmt(k.pm25Avg, 1);
  $('k-avg').textContent = `期间平均 AQI ${fmt(k.avgAqi, 1)}`;
  $('k-primary').textContent = k.primary;
  $('k-peak').textContent = `AQI 峰值 ${k.maxAqi}（${k.maxAqiDate}）`;
}

function renderTrend(daily) {
  const d = daily.data;
  const unit = daily.meta.unit;
  const c = baseChart('c-trend');
  c.setOption({
    grid: { left: 46, right: 56, top: 34, bottom: 26 },
    tooltip: Object.assign({
      trigger: 'axis',
      axisPointer: { type: 'line', lineStyle: { color: 'rgba(91,155,213,.4)' } },
      formatter: p => {
        const row = d[p[0].dataIndex];
        return `<b>${row.date}</b>　${row.level}<br/>` +
          `AQI：<b>${row.aqi}</b>（${unit.aqi}）<br/>` +
          `PM2.5：<b>${fmt(row['PM2.5'], 1)}</b> ${unit['PM2.5']}<br/>` +
          `PM10：${fmt(row['PM10'], 1)} ${unit['PM10']}<br/>` +
          `O3：${fmt(row['O3'], 1)} ${unit['O3']}<br/>` +
          `首要污染物：${row.primary}`;
      }
    }, TOOLTIP),
    legend: {
      data: ['AQI', 'PM2.5'], right: 12, top: 4, itemWidth: 14, itemHeight: 8,
      textStyle: { color: '#5a7590', fontSize: 11 }
    },
    xAxis: {
      type: 'category',
      data: d.map(x => x.date.slice(5)),
      boundaryGap: false,
      axisLine: { lineStyle: { color: 'rgba(91,155,213,.35)' } },
      axisTick: { show: false },
      axisLabel: { color: AXIS, fontSize: 9, interval: 4 }
    },
    yAxis: [
      {
        type: 'value', name: 'AQI', nameTextStyle: { color: AXIS, fontSize: 10, padding: [0, 0, 4, 0] },
        min: 0, max: 300, splitNumber: 4,
        axisLabel: { color: AXIS, fontSize: 9 },
        splitLine: { lineStyle: { color: GRID_LINE } }
      },
      {
        type: 'value', name: 'μg/m³', nameTextStyle: { color: '#5fb878', fontSize: 10, padding: [0, 0, 4, 0] },
        min: 0, splitNumber: 4,
        axisLabel: { color: '#5fb878', fontSize: 9 },
        splitLine: { show: false }
      }
    ],
    series: [
      {
        name: 'AQI', type: 'line', smooth: true, symbol: 'none',
        data: d.map(x => x.aqi),
        lineStyle: { width: 2, color: '#4a90c4', shadowColor: 'rgba(74,144,196,.5)', shadowBlur: 8 },
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: 'rgba(91,155,213,.30)' },
            { offset: 1, color: 'rgba(91,155,213,.03)' }
          ])
        },
        markLine: {
          silent: true, symbol: 'none',
          lineStyle: { type: 'dashed', width: 1 },
          data: [
            { yAxis: 100, lineStyle: { color: 'rgba(214,158,46,.6)' },
              label: { formatter: '优良线 100', color: '#b8860b', fontSize: 9, position: 'insideEndTop' } },
            { yAxis: 200, lineStyle: { color: 'rgba(217,83,79,.6)' },
              label: { formatter: '重污染线 200', color: '#c0392b', fontSize: 9, position: 'insideEndTop' } }
          ]
        }
      },
      {
        name: 'PM2.5', type: 'line', yAxisIndex: 1, smooth: true, symbol: 'none',
        data: d.map(x => x['PM2.5']),
        lineStyle: { width: 1.6, color: '#5fb878' }
      }
    ]
  }, true);
}

function renderLevel(level, kpi) {
  const rows = level.data;
  const c = baseChart('c-level');
  const legendNames = rows.map(l => ({
    name: `${l.level} ${l.days}天`,
    textStyle: { color: l.days > 0 ? '#5a7590' : '#b0bfcc' },
    icon: 'circle'
  }));
  const pieData = rows.map(l => ({
    name: `${l.level} ${l.days}天`,
    value: l.days,
    ratio: l.ratio,
    label: { show: l.days > 0 },
    labelLine: { show: l.days > 0 },
    itemStyle: { color: soften(l.color), borderColor: '#f3f8fc', borderWidth: 2 }
  }));

  c.setOption({
    tooltip: Object.assign({
      trigger: 'item',
      formatter: p => {
        const l = rows.find(x => `${x.level} ${x.days}天` === p.name) || {};
        return `<b>${p.name}</b><br/>天数：${l.days} 天<br/>占比：${fmt(l.ratio, 2)}%<br/>` +
          `AQI：${l.lo}~${l.hi}<br/><span style="color:#5a7590">${l.advice || ''}</span>`;
      }
    }, TOOLTIP),
    title: {
      text: String(kpi.data.validDays), subtext: '有效天',
      left: '50%', top: '39%', textAlign: 'center',
      textStyle: { color: '#2d6fae', fontSize: 26, fontWeight: 700, fontFamily: 'Bahnschrift,monospace' },
      subtextStyle: { color: '#7d93a6', fontSize: 11 }
    },
    legend: [
      {
        bottom: 16, left: 'center', itemWidth: 8, itemHeight: 8, itemGap: 10,
        icon: 'circle', textStyle: { color: '#5a7590', fontSize: 9 },
        data: legendNames.slice(0, 3)
      },
      {
        bottom: 2, left: 'center', itemWidth: 8, itemHeight: 8, itemGap: 10,
        icon: 'circle', textStyle: { color: '#5a7590', fontSize: 9 },
        data: legendNames.slice(3)
      }
    ],
    series: [{
      type: 'pie', radius: ['46%', '64%'], center: ['50%', '45%'],
      avoidLabelOverlap: true, startAngle: 90,
      itemStyle: { borderColor: '#f3f8fc', borderWidth: 2 },
      label: {
        color: '#3a5b78', fontSize: 10, lineHeight: 13,
        formatter: p => `${p.name.split(' ')[0]}\n${fmt(p.data.ratio, 1)}%`
      },
      labelLine: { length: 6, length2: 8, lineStyle: { color: 'rgba(120,150,170,.45)' } },
      data: pieData
    }]
  }, true);
}

function renderPrimary(primary) {
  const rows = primary.data;
  const palette = {
    'PM2.5': '#5b9bd5', 'PM10': '#7fc4e8', 'NO2': '#5fb878',
    'O3': '#3bc1c8', 'SO2': '#f4c56a', '无': '#b6c4d0'
  };
  const c = baseChart('c-primary');
  c.setOption({
    tooltip: Object.assign({
      trigger: 'item',
      formatter: p => `<b>${p.name}</b><br/>站点日：${p.value} 个<br/>占比：${fmt(p.data.ratio, 2)}%`
    }, TOOLTIP),
    legend: {
      bottom: 2, left: 'center', itemWidth: 9, itemHeight: 9, itemGap: 10,
      icon: 'circle', textStyle: { color: '#5a7590', fontSize: 10 }
    },
    series: [{
      type: 'pie', roseType: 'radius', radius: ['14%', '56%'], center: ['50%', '44%'],
      itemStyle: { borderColor: '#f3f8fc', borderWidth: 2, borderRadius: 3 },
      label: {
        color: '#3a5b78', fontSize: 10,
        formatter: p => `${p.name === '无' ? '无首要' : p.name}\n${fmt(p.data.ratio, 1)}%`
      },
      labelLine: { length: 5, length2: 7, lineStyle: { color: 'rgba(120,150,170,.45)' } },
      data: rows.map(r => ({
        name: r.name === '无' ? '无' : r.name,
        value: r.records,
        ratio: r.ratio,
        itemStyle: { color: palette[r.name] || '#b0bec5' }
      }))
    }]
  }, true);
}

function renderRank(rank) {
  const rows = rank.data.worst.slice().sort((a, b) => a.avgAqi - b.avgAqi);
  const nameCount = {};
  rows.forEach(r => { nameCount[r.name] = (nameCount[r.name] || 0) + 1; });
  const yLabel = r => {
    const s = nameCount[r.name] > 1 ? `${r.name}·${r.code}` : r.name;
    return s.length > 16 ? s.slice(0, 15) + '…' : s;
  };
  const c = baseChart('c-rank');
  c.setOption({
    grid: { left: 152, right: 52, top: 12, bottom: 22 },
    tooltip: Object.assign({
      trigger: 'axis', axisPointer: { type: 'shadow' },
      formatter: p => {
        const r = rows[p[0].dataIndex];
        return `<b>${r.name}</b>（${r.code}）<br/>区县：${r.county}　类型：${r.type}<br/>` +
          `平均 AQI：<b>${fmt(r.avgAqi, 1)}</b><br/>优良率：${fmt(r.goodRate, 2)}%<br/>` +
          `峰值 AQI：${r.maxAqi}<br/>PM2.5 均值：${fmt(r.pm25, 1)} μg/m³`;
      }
    }, TOOLTIP),
    xAxis: {
      type: 'value', min: 80, max: 100,
      axisLine: { show: false }, axisTick: { show: false },
      axisLabel: { color: AXIS, fontSize: 9 },
      splitLine: { lineStyle: { color: GRID_LINE } }
    },
    yAxis: {
      type: 'category',
      data: rows.map(yLabel),
      axisLine: { lineStyle: { color: 'rgba(91,155,213,.35)' } },
      axisTick: { show: false },
      axisLabel: { color: '#5a7590', fontSize: 9 }
    },
    series: [{
      type: 'bar', barWidth: 11,
      data: rows.map(r => ({
        value: r.avgAqi,
        itemStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
            { offset: 0, color: hexA(levelColor(r.avgAqi), 0.42) },
            { offset: 1, color: levelColor(r.avgAqi) }
          ]),
          borderRadius: [0, 2, 2, 0]
        }
      })),
      label: {
        show: true, position: 'right', color: '#2b4a66', fontSize: 10, fontWeight: 600,
        formatter: p => fmt(p.value, 1)
      },
      markLine: {
        silent: true, symbol: 'none',
        lineStyle: { type: 'dashed', width: 1, color: 'rgba(214,158,46,.6)' },
        data: [{ xAxis: 100, label: { formatter: '优良线', color: '#b8860b', fontSize: 9, position: 'end' } }]
      }
    }]
  }, true);
}

function renderScatter(weather) {
  const data = weather.data.scatter;
  const r = weather.meta.corr.wind;
  const c = baseChart('c-scatter');

  const xs = data.map(d => d.wind), ys = data.map(d => d.aqi);
  const n = xs.length;
  const mx = xs.reduce((a, b) => a + b, 0) / n, my = ys.reduce((a, b) => a + b, 0) / n;
  let sxy = 0, sxx = 0;
  for (let i = 0; i < n; i++) { sxy += (xs[i] - mx) * (ys[i] - my); sxx += (xs[i] - mx) ** 2; }
  const slope = sxx === 0 ? 0 : sxy / sxx, intercept = my - slope * mx;
  const x0 = Math.min(...xs), x1 = Math.max(...xs);

  c.setOption({
    grid: { left: 46, right: 56, top: 34, bottom: 30 },
    tooltip: Object.assign({
      trigger: 'item',
      formatter: p => {
        const d = p.data[2];
        return `<b>${d.date}</b>　${d.county}<br/>天气：${d.weather}<br/>` +
          `风速：<b>${fmt(d.wind, 2)}</b> m/s<br/>日均 AQI：<b>${d.aqi}</b>（${levelName(d.aqi)}）<br/>` +
          `湿度：${fmt(d.humidity, 1)}%　气温：${fmt(d.temp, 1)} ℃`;
      }
    }, TOOLTIP),
    xAxis: {
      type: 'value', name: '风速 m/s', nameTextStyle: { color: AXIS, fontSize: 10 },
      scale: true, axisLine: { lineStyle: { color: 'rgba(91,155,213,.35)' } },
      axisLabel: { color: AXIS, fontSize: 9 },
      splitLine: { lineStyle: { color: GRID_LINE } }
    },
    yAxis: {
      type: 'value', name: 'AQI', nameTextStyle: { color: AXIS, fontSize: 10 },
      scale: true, axisLine: { show: false },
      axisLabel: { color: AXIS, fontSize: 9 },
      splitLine: { lineStyle: { color: GRID_LINE } }
    },
    series: [
      {
        type: 'scatter', symbolSize: 8,
        data: data.map(d => ({
          value: [d.wind, d.aqi],
          date: d, itemStyle: { color: levelColor(d.aqi), opacity: 0.8 }
        })),
        emphasis: { itemStyle: { borderColor: '#2d6fae', borderWidth: 1 } }
      },
      {
        type: 'line', symbol: 'none', silent: true, z: 1,
        data: [[x0, slope * x0 + intercept], [x1, slope * x1 + intercept]],
        lineStyle: { color: 'rgba(91,155,213,.75)', width: 1.4, type: 'dashed' }
      }
    ],
    graphic: [{
      type: 'text', right: 62, top: 8,
      style: {
        text: `r = ${fmt(r, 4)}　样本 ${weather.meta.pairs}`,
        fill: '#2d6fae', fontSize: 12, fontFamily: 'Bahnschrift,monospace'
      }
    }]
  }, true);
}

function renderMonth(month) {
  const rows = month.data;
  const names = rows.map(r => r.pollutant);
  const c = baseChart('c-month');

  const isCO = rows.map(r => r.pollutant === 'CO');
  const left1 = rows.map((r, i) => isCO[i] ? null : r.m1);
  const left2 = rows.map((r, i) => isCO[i] ? null : r.m2);
  const right1 = rows.map((r, i) => isCO[i] ? r.m1 : null);
  const right2 = rows.map((r, i) => isCO[i] ? r.m2 : null);

  c.setOption({
    grid: { left: 44, right: 44, top: 38, bottom: 22 },
    tooltip: Object.assign({
      trigger: 'axis', axisPointer: { type: 'shadow' },
      formatter: ps => {
        const i = ps[0].dataIndex, r = rows[i];
        return `<b>${r.pollutant}</b>　单位 ${r.unit}<br/>` +
          `${month.meta.months[0]}：<b>${fmt(r.m1, 1)}</b><br/>` +
          `${month.meta.months[1]}：<b>${fmt(r.m2, 1)}</b><br/>` +
          `变化：${r.change > 0 ? '+' : ''}${fmt(r.change, 1)}（${fmt(r.changeRate, 2)}%）`;
      }
    }, TOOLTIP),
    legend: {
      data: ['10 月', '11 月', 'CO 10 月', 'CO 11 月'],
      top: 4, left: 'center', itemWidth: 12, itemHeight: 8, itemGap: 14,
      textStyle: { color: '#5a7590', fontSize: 10 }
    },
    xAxis: {
      type: 'category', data: names,
      axisLine: { lineStyle: { color: 'rgba(91,155,213,.35)' } },
      axisTick: { show: false },
      axisLabel: { color: '#5a7590', fontSize: 11 }
    },
    yAxis: [
      {
        type: 'value',
        axisLabel: { color: AXIS, fontSize: 9, formatter: '{value}' },
        splitLine: { lineStyle: { color: GRID_LINE } }
      },
      {
        type: 'value',
        axisLabel: { color: '#5fb878', fontSize: 9 },
        splitLine: { show: false }
      }
    ],
    series: [
      {
        name: '10 月', type: 'bar', barGap: '12%', barWidth: 16,
        data: left1,
        itemStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: '#5b9bd5' }, { offset: 1, color: 'rgba(91,155,213,.25)' }]),
          borderRadius: [2, 2, 0, 0] }
      },
      {
        name: '11 月', type: 'bar', barWidth: 16,
        data: left2,
        itemStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: '#5fb878' }, { offset: 1, color: 'rgba(95,184,120,.25)' }]),
          borderRadius: [2, 2, 0, 0] }
      },
      {
        name: 'CO 10 月', type: 'bar', yAxisIndex: 1, barWidth: 16,
        data: right1,
        itemStyle: { color: '#3bc1c8', borderRadius: [2, 2, 0, 0], opacity: .85 }
      },
      {
        name: 'CO 11 月', type: 'bar', yAxisIndex: 1, barWidth: 16,
        data: right2,
        itemStyle: { color: '#e8c885', borderRadius: [2, 2, 0, 0], opacity: .85 }
      }
    ]
  }, true);
}

function renderCalendar(cal) {
  const counties = cal.meta.counties;
  const cells = cal.data;
  const months = [...new Set(cells.map(c => c[0].slice(0, 7)))].sort();
  const el = $('c-calendar');
  const c = baseChart('c-calendar');

  const W = el.clientWidth || 1890;
  const H = el.clientHeight || 250;
  const gap = 10;
  const padL = 34;
  const padR = 10;
  const calW = Math.floor((W - padL - padR - gap * (counties.length - 1)) / counties.length);
  const topPad = 50;
  const bottomPad = 34;

  const d0 = new Date(cells[0][0] + 'T00:00:00');
  const d1 = new Date(cells[cells.length - 1][0] + 'T00:00:00');
  const offset = (d0.getDay() + 6) % 7;
  const totalDays = Math.round((d1 - d0) / 86400000) + 1 + offset;
  const weeks = Math.ceil(totalDays / 7);
  const cellW = Math.max(14, Math.floor((calW - 6) / weeks));
  const cellH = Math.max(12, Math.floor((H - topPad - bottomPad - 18) / 7));

  const calendars = counties.map((name, i) => ({
    top: topPad,
    left: padL + i * (calW + gap),
    width: calW,
    height: H - topPad - bottomPad,
    cellSize: [cellW, cellH],
    range: months,
    orient: 'horizontal',
    dayLabel: {
      show: true, firstDay: 1, nameMap: 'ZH',
      color: '#6b8296', fontSize: 9, margin: 5
    },
    monthLabel: {
      show: true, nameMap: 'ZH',
      color: '#4a6b85', fontSize: 10, margin: 6,
      position: 'start'
    },
    yearLabel: { show: false },
    splitLine: { show: false },
    itemStyle: { color: 'transparent', borderWidth: 0 }
  }));

  const series = counties.map((name, i) => ({
    name, type: 'heatmap', coordinateSystem: 'calendar', calendarIndex: i,
    data: cells.filter(x => x[1] === name).map(x => [x[0], x[2]]),
    itemStyle: { borderRadius: 2, borderWidth: 1.5, borderColor: 'rgba(238,245,251,.95)' }
  }));

  const graphics = counties.map((name, i) => ({
    type: 'text',
    left: padL + i * (calW + gap) + calW / 2 - 26,
    top: 4,
    style: { text: name, fill: '#4a6b85', fontSize: 12, fontWeight: 600 }
  }));

  c.setOption({
    tooltip: Object.assign({
      formatter: p => `<b>${p.value[0]}</b>　${p.seriesName}<br/>日均 AQI：<b>${p.value[1]}</b>（${levelName(p.value[1])}）`
    }, TOOLTIP),
    visualMap: {
      type: 'piecewise', orient: 'horizontal', left: 'center', bottom: 4,
      itemWidth: 11, itemHeight: 9, itemGap: 10, textGap: 4,
      textStyle: { color: '#5a7590', fontSize: 10 },
      pieces: cal.meta.levels.map(l => ({
        gte: l.lo, lte: l.hi, label: `${l.level} ${l.lo}~${l.hi}`, color: soften(l.color)
      }))
    },
    calendar: calendars,
    series: series,
    graphic: graphics
  }, true);
}

const STAGE_W = 1920, STAGE_H = 1080;

function fitStage() {
  const st = document.querySelector('.stage');
  if (!st) return;
  const vw = window.innerWidth, vh = window.innerHeight;
  const s = Math.min(vw / STAGE_W, vh / STAGE_H);
  const x = (vw - STAGE_W * s) / 2;
  const y = (vh - STAGE_H * s) / 2;
  st.style.transform = `translate(${x.toFixed(2)}px, ${y.toFixed(2)}px) scale(${s.toFixed(5)})`;
}

window.addEventListener('resize', fitStage);
fitStage();

function showError(e) {
  const hint = $('err-hint');
  if (location.protocol === 'file:') {
    hint.innerHTML =
      '浏览器当前地址是 <code>file:///</code> —— 说明这个页面是被<b>直接双击 HTML</b> 打开的。<br>' +
      '这种方式下浏览器禁止页面读取本地的 <code>output/*.json</code>，所以数据出不来，页面本身没坏。<br>' +
      '正确做法：回到上一级 <code>1_项目源代码</code> 目录，双击 <code>打开大屏.bat</code>，<br>' +
      '它会起一个本地服务并自动打开 <code>http://127.0.0.1:8080/dashboard/index.html</code>。';
  } else {
    hint.innerHTML =
      '页面地址是 <code>' + location.origin + location.pathname + '</code>，但读不到 <code>output/*.json</code>。<br>' +
      '请依次确认：① <code>1_项目源代码\\output</code> 下有 11 个 JSON（若为空先运行 <code>run.bat</code>）；<br>' +
      '② 服务是在 <code>1_项目源代码</code> 目录启动的（不是 <code>dashboard</code> 目录）；③ 端口是 8080 还是 8081。';
  }
  $('err').classList.add('show');
  $('err-detail').textContent = (e && e.message) ? e.message : String(e);
}

window.addEventListener('load', async () => {
  try {
    await loadAll(true);
    window.addEventListener('resize', () => Object.values(charts).forEach(c => c.resize()));
    setInterval(() => loadAll(false).catch(showError), 60000);
  } catch (e) {
    showError(e);
  }
});
