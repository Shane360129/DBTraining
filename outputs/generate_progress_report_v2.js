const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, LevelFormat,
  HeadingLevel, BorderStyle, WidthType, ShadingType,
  PageNumber, PageBreak, TabStopType, TabStopPosition
} = require("docx");

// ============================================================
// Constants & Helpers
// ============================================================
const PAGE_WIDTH = 11906; // A4
const PAGE_HEIGHT = 16838;
const MARGIN = 1440;
const CONTENT_WIDTH = PAGE_WIDTH - 2 * MARGIN; // 9026

const BLUE = "1F4E79";
const LIGHT_BLUE = "D6E4F0";
const DARK_GRAY = "333333";
const MEDIUM_GRAY = "666666";
const LIGHT_GRAY = "F2F2F2";

const border = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const borders = { top: border, bottom: border, left: border, right: border };

function headerCell(text, width) {
  return new TableCell({
    borders,
    width: { size: width, type: WidthType.DXA },
    shading: { fill: BLUE, type: ShadingType.CLEAR },
    margins: { top: 60, bottom: 60, left: 100, right: 100 },
    verticalAlign: "center",
    children: [new Paragraph({
      alignment: AlignmentType.CENTER,
      children: [new TextRun({ text, bold: true, font: "Microsoft JhengHei", size: 20, color: "FFFFFF" })]
    })]
  });
}

function dataCell(text, width, opts = {}) {
  return new TableCell({
    borders,
    width: { size: width, type: WidthType.DXA },
    shading: opts.shading ? { fill: opts.shading, type: ShadingType.CLEAR } : undefined,
    margins: { top: 50, bottom: 50, left: 100, right: 100 },
    verticalAlign: "center",
    children: [new Paragraph({
      alignment: opts.center ? AlignmentType.CENTER : AlignmentType.LEFT,
      children: [new TextRun({
        text,
        font: opts.mono ? "Consolas" : "Microsoft JhengHei",
        size: opts.small ? 18 : 20,
        bold: opts.bold || false,
        color: opts.color || DARK_GRAY
      })]
    })]
  });
}

// Multi-line cell
function multiCell(lines, width, opts = {}) {
  return new TableCell({
    borders,
    width: { size: width, type: WidthType.DXA },
    shading: opts.shading ? { fill: opts.shading, type: ShadingType.CLEAR } : undefined,
    margins: { top: 50, bottom: 50, left: 100, right: 100 },
    children: lines.map(l => new Paragraph({
      spacing: { after: 40 },
      children: [new TextRun({
        text: l.text || l,
        font: l.mono ? "Consolas" : "Microsoft JhengHei",
        size: l.size || 18,
        bold: l.bold || false,
        color: l.color || DARK_GRAY
      })]
    }))
  });
}

function heading1(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_1,
    spacing: { before: 360, after: 200 },
    children: [new TextRun({ text, bold: true, font: "Microsoft JhengHei", size: 32, color: BLUE })]
  });
}

function heading2(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_2,
    spacing: { before: 280, after: 160 },
    children: [new TextRun({ text, bold: true, font: "Microsoft JhengHei", size: 26, color: BLUE })]
  });
}

function heading3(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_3,
    spacing: { before: 200, after: 120 },
    children: [new TextRun({ text, bold: true, font: "Microsoft JhengHei", size: 22, color: "2E75B6" })]
  });
}

function bodyText(text, opts = {}) {
  return new Paragraph({
    spacing: { after: opts.noSpace ? 0 : 120, line: 360 },
    indent: opts.indent ? { left: opts.indent } : undefined,
    children: [new TextRun({ text, font: "Microsoft JhengHei", size: 22, color: DARK_GRAY, bold: opts.bold || false })]
  });
}

function codeBlock(text) {
  return new Paragraph({
    spacing: { after: 80, line: 300 },
    indent: { left: 360 },
    shading: { fill: "F5F5F5", type: ShadingType.CLEAR },
    children: [new TextRun({ text, font: "Consolas", size: 18, color: "2E4057" })]
  });
}

function bulletItem(text, level = 0) {
  return new Paragraph({
    numbering: { reference: "bullets", level },
    spacing: { after: 80, line: 340 },
    children: [new TextRun({ text, font: "Microsoft JhengHei", size: 22, color: DARK_GRAY })]
  });
}

function numberItem(text, level = 0) {
  return new Paragraph({
    numbering: { reference: "numbers", level },
    spacing: { after: 80, line: 340 },
    children: [new TextRun({ text, font: "Microsoft JhengHei", size: 22, color: DARK_GRAY })]
  });
}

function emptyLine() {
  return new Paragraph({ spacing: { after: 60 }, children: [] });
}

function divider() {
  return new Paragraph({
    spacing: { before: 200, after: 200 },
    border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: LIGHT_BLUE, space: 1 } },
    children: []
  });
}

// Explanation box (light blue background)
function explainBox(text) {
  return new Paragraph({
    spacing: { after: 120, line: 340 },
    indent: { left: 360 },
    shading: { fill: "E8F4FD", type: ShadingType.CLEAR },
    border: { left: { style: BorderStyle.SINGLE, size: 12, color: "2E75B6", space: 4 } },
    children: [new TextRun({ text, font: "Microsoft JhengHei", size: 20, color: "1B4F72", italics: true })]
  });
}

// ============================================================
// Build Document
// ============================================================
const doc = new Document({
  styles: {
    default: { document: { run: { font: "Microsoft JhengHei", size: 22 } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 32, bold: true, font: "Microsoft JhengHei", color: BLUE },
        paragraph: { spacing: { before: 360, after: 200 }, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 26, bold: true, font: "Microsoft JhengHei", color: BLUE },
        paragraph: { spacing: { before: 280, after: 160 }, outlineLevel: 1 } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 22, bold: true, font: "Microsoft JhengHei", color: "2E75B6" },
        paragraph: { spacing: { before: 200, after: 120 }, outlineLevel: 2 } },
    ]
  },
  numbering: {
    config: [
      { reference: "bullets", levels: [
        { level: 0, format: LevelFormat.BULLET, text: "\u2022", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 } } } },
        { level: 1, format: LevelFormat.BULLET, text: "\u25E6", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 1440, hanging: 360 } } } },
      ]},
      { reference: "numbers", levels: [
        { level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: 720, hanging: 360 } } } },
      ]},
    ]
  },
  sections: [
    // ========== COVER PAGE ==========
    {
      properties: {
        page: { size: { width: PAGE_WIDTH, height: PAGE_HEIGHT }, margin: { top: 2880, right: MARGIN, bottom: MARGIN, left: MARGIN } }
      },
      children: [
        new Paragraph({ spacing: { after: 600 }, children: [] }),
        new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 200 },
          children: [new TextRun({ text: "研究進度報告", font: "Microsoft JhengHei", size: 52, bold: true, color: BLUE })] }),
        new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 100 },
          border: { bottom: { style: BorderStyle.SINGLE, size: 8, color: BLUE, space: 8 } },
          children: [new TextRun({ text: "Research Progress Report", font: "Arial", size: 28, color: MEDIUM_GRAY, italics: true })] }),
        new Paragraph({ spacing: { after: 600 }, children: [] }),
        new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 200 },
          children: [new TextRun({ text: "企業級 Text-to-SQL 落地實驗", font: "Microsoft JhengHei", size: 36, color: DARK_GRAY })] }),
        new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 100 },
          children: [new TextRun({ text: "基於 Llama-3.1-8B + DoRA 微調之繁體中文自然語言轉 T-SQL 系統", font: "Microsoft JhengHei", size: 22, color: MEDIUM_GRAY })] }),
        new Paragraph({ spacing: { after: 600 }, children: [] }),
        new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 120 },
          children: [new TextRun({ text: "報告日期：2026 年 3 月 22 日", font: "Microsoft JhengHei", size: 24, color: DARK_GRAY })] }),
        new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 120 },
          children: [new TextRun({ text: "預計完成日期：2026 年 5 月初", font: "Microsoft JhengHei", size: 24, color: DARK_GRAY })] }),
      ]
    },

    // ========== MAIN CONTENT ==========
    {
      properties: {
        page: { size: { width: PAGE_WIDTH, height: PAGE_HEIGHT }, margin: { top: MARGIN, right: MARGIN, bottom: MARGIN, left: MARGIN } }
      },
      headers: {
        default: new Header({ children: [new Paragraph({
          border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: LIGHT_BLUE, space: 4 } },
          tabStops: [{ type: TabStopType.RIGHT, position: TabStopPosition.MAX }],
          children: [
            new TextRun({ text: "企業級 Text-to-SQL 研究進度報告", font: "Microsoft JhengHei", size: 18, color: MEDIUM_GRAY }),
            new TextRun({ text: "\t2026.03.22", font: "Microsoft JhengHei", size: 18, color: MEDIUM_GRAY }),
          ]
        })] })
      },
      footers: {
        default: new Footer({ children: [new Paragraph({
          alignment: AlignmentType.CENTER,
          border: { top: { style: BorderStyle.SINGLE, size: 2, color: LIGHT_BLUE, space: 4 } },
          children: [
            new TextRun({ text: "Page ", font: "Arial", size: 18, color: MEDIUM_GRAY }),
            new TextRun({ children: [PageNumber.CURRENT], font: "Arial", size: 18, color: MEDIUM_GRAY }),
          ]
        })] })
      },
      children: [

// =====================================================================
// PART 1: RESEARCH PROGRESS
// =====================================================================
heading1("第一部分：研究進度總覽"),
divider(),

heading2("1.1 研究主題與目標"),
bodyText("本研究旨在探索大型語言模型（LLM）於企業級資料庫的 Text-to-SQL 落地應用。以實際生產環境中的 WP_M09 資料庫（SQL Server, 7 個 View）為實驗場域，採用 Llama-3.1-8B-Instruct 搭配 DoRA (Weight-Decomposed Low-Rank Adaptation) 微調技術，仿照 Spider 1.0 與 BIRD Benchmark 的訓練方法論，建立一套可處理繁體中文自然語言查詢並轉換為 T-SQL 的系統。"),
emptyLine(),
bodyText("研究目標：", { bold: true }),
bulletItem("驗證 Spider/BIRD 方法論於企業環境的可行性與效度"),
bulletItem("達成 Execution Accuracy (EX) 目標，解決已知的表選擇失敗、子查詢去重等瓶頸"),
bulletItem("建立完整的 Ablation 實驗框架，量化各技術貢獻的影響"),
bulletItem("完成學術論文撰寫，提出企業級 Text-to-SQL 落地方法論"),

heading2("1.2 研究歷程概覽"),
bodyText("自 2026 年 2 月底開始至今，研究歷經四個主要階段："),
new Table({
  width: { size: CONTENT_WIDTH, type: WidthType.DXA },
  columnWidths: [1800, 2200, 2513, 2513],
  rows: [
    new TableRow({ children: [headerCell("階段", 1800), headerCell("時間", 2200), headerCell("方法", 2513), headerCell("最佳結果", 2513)] }),
    new TableRow({ children: [
      dataCell("Phase 1：探索期", 1800, { bold: true }),
      dataCell("02/28 - 03/06", 2200, { center: true }),
      dataCell("LoRA/DoRA, Schema+Q+SQL 格式", 2513),
      dataCell("EM 53%（舊測試集）", 2513),
    ]}),
    new TableRow({ children: [
      dataCell("Phase 2：Spider 格式", 1800, { bold: true, shading: LIGHT_GRAY }),
      dataCell("03/08 - 03/15", 2200, { center: true, shading: LIGHT_GRAY }),
      dataCell("Spider 1.0 格式, 單表 schema", 2513, { shading: LIGHT_GRAY }),
      dataCell("EM 67%（舊測試集）", 2513, { shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("Phase 3：資料擴增", 1800, { bold: true }),
      dataCell("03/17 - 03/20", 2200, { center: true }),
      dataCell("Auto Loop, 大量擴增訓練資料", 2513),
      dataCell("EM 91.76%（舊）/ EX 44.12%（新）", 2513),
    ]}),
    new TableRow({ children: [
      dataCell("Phase 4：Enterprise", 1800, { bold: true, shading: LIGHT_BLUE }),
      dataCell("03/21 - 進行中", 2200, { center: true, shading: LIGHT_BLUE }),
      dataCell("全 Schema + 商業規則 + 弱模式擴增", 2513, { shading: LIGHT_BLUE }),
      dataCell("訓練中（預計 03/23 完成）", 2513, { shading: LIGHT_BLUE }),
    ]}),
  ]
}),

heading2("1.3 已完成工作"),
heading3("A. 資料庫分析與 Schema 設計"),
bulletItem("完成 WP_M09 資料庫 7 個 View 的完整分析（欄位、關聯、商業邏輯）"),
bulletItem("識別 isDel/dtlIsDel 軟刪除機制：4 個 View 有（AcctIn, AcctOut, OutStock, Transfer）、3 個無"),
bulletItem("歸納 4 條核心商業規則：isDel 過濾、子查詢去重、日期篩選、pNo 非日期"),
bulletItem("設計緊湊 Schema 格式：從 CREATE TABLE (~1,614 tokens) 壓縮至欄位列表 (~937 tokens)"),

heading3("B. 訓練資料建構"),
bulletItem("train_spider_WP_M09.json：1,014 筆，7 View 均衡分布（132-155/view）"),
bulletItem("train_claude_en_2000.json：1,748 筆，英文 54% / 中文 46%"),
bulletItem("手工設計 50 筆弱模式擴增資料（OutStock 18、子查詢 11、isDel 12、DISTINCT 5、Transfer 4）"),
bulletItem("去重後合計 2,585 筆訓練樣本"),
bulletItem("建立新版驗證集 val_claude_en_spider_v2.json（238 筆，34/view，全部通過 DB 執行驗證）"),

heading3("C. 模型訓練（15 個版本）"),
bulletItem("從 Phase 1 到 Phase 4 共訓練 15 個模型版本"),
bulletItem("基礎架構：Llama-3.1-8B-Instruct + DoRA (r=16, alpha=32) + 4-bit 量化"),
bulletItem("超參數演進：LR 3e-4 -> 5e-5, Epochs 3-10 -> 6, SEQ_LEN 512 -> 1280"),

heading3("D. 評估框架"),
bulletItem("建立新版驗證集（238 筆），消除舊測試集的資料洩漏風險"),
bulletItem("評估指標：Table Selection Accuracy + String EM + Execution Accuracy (EX)"),
bulletItem("多維度分析：per-view（7 個 View）、per-difficulty（easy/medium/hard）"),

// =====================================================================
// 1.4 KEY FINDINGS
// =====================================================================
heading2("1.4 關鍵發現與問題診斷"),
emptyLine(),
new Table({
  width: { size: CONTENT_WIDTH, type: WidthType.DXA },
  columnWidths: [2200, 2413, 2413, 2000],
  rows: [
    new TableRow({ children: [headerCell("問題", 2200), headerCell("根因", 2413), headerCell("解決方案", 2413), headerCell("狀態", 2000)] }),
    new TableRow({ children: [
      dataCell("OutStock EX 僅 5.9%", 2200, { bold: true }),
      dataCell("Keyword 推斷表失敗（31/32 錯誤）", 2413),
      dataCell("全 7 表 Schema，模型自行選表", 2413),
      dataCell("Enterprise v0322 已導入", 2000, { color: "2E75B6" }),
    ]}),
    new TableRow({ children: [
      dataCell("子查詢去重 0%", 2200, { bold: true, shading: LIGHT_GRAY }),
      dataCell("Claude 訓練集 0 筆子查詢", 2413, { shading: LIGHT_GRAY }),
      dataCell("擴增 11 筆子查詢範例", 2413, { shading: LIGHT_GRAY }),
      dataCell("Enterprise v0322 已導入", 2000, { color: "2E75B6", shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("isDel/dtlIsDel 混淆", 2200, { bold: true }),
      dataCell("無明確規則指引", 2413),
      dataCell("Business Rules + 12 筆成對範例", 2413),
      dataCell("Enterprise v0322 已導入", 2000, { color: "2E75B6" }),
    ]}),
    new TableRow({ children: [
      dataCell("舊測試集 EM 91.76% 不可靠", 2200, { bold: true, shading: LIGHT_GRAY }),
      dataCell("訓練/測試集可能存在資料洩漏", 2413, { shading: LIGHT_GRAY }),
      dataCell("建立新版驗證集（238 筆，重疊率 5%）", 2413, { shading: LIGHT_GRAY }),
      dataCell("已完成", 2000, { color: "008000", shading: LIGHT_GRAY }),
    ]}),
  ]
}),

// =====================================================================
// NEW: 1.5 TRAINING & VALIDATION DATA SAMPLES
// =====================================================================
new Paragraph({ children: [new PageBreak()] }),
heading2("1.5 訓練集與驗證集內容展示"),

heading3("A. 訓練集 — train_spider_WP_M09.json（1,014 筆）範例"),
bodyText("此為人工生成的 Spider 格式訓練資料，涵蓋 7 個 View 的基礎查詢模式："),
emptyLine(),

new Table({
  width: { size: CONTENT_WIDTH, type: WidthType.DXA },
  columnWidths: [3600, 5426],
  rows: [
    new TableRow({ children: [headerCell("自然語言問句 (Question)", 3600), headerCell("目標 SQL (Query)", 5426)] }),
    new TableRow({ children: [
      dataCell("Retrieve the total accounts receivable amount for receipt ID '202512050001'.", 3600, { small: true }),
      dataCell("SELECT TOP 1 amount FROM WP_M09.dbo.WP_vAcctIn WHERE acctInId = '202512050001' AND isDel = 'N';", 5426, { mono: true, small: true }),
    ]}),
    new TableRow({ children: [
      dataCell("What is the true total receivable amount collected on December 5, 2025?", 3600, { small: true, shading: LIGHT_GRAY }),
      dataCell("SELECT SUM(UniqueAmount) FROM (SELECT acctInId, MAX(amount) AS UniqueAmount FROM WP_M09.dbo.WP_vAcctIn WHERE LEFT(acctInId, 8) = '20251205' AND isDel = 'N' GROUP BY acctInId) AS AR_Totals;", 5426, { mono: true, small: true, shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("List all product barcodes transferred on October 27, 2025.", 3600, { small: true }),
      dataCell("SELECT DISTINCT pBarcode FROM WP_M09.dbo.WP_vTransfer WHERE LEFT(TransferId, 8) = '20251027' AND isDel = 'N' AND dtlIsDel = 'N';", 5426, { mono: true, small: true }),
    ]}),
    new TableRow({ children: [
      dataCell("Retrieve the true total sales amount for out-stock ID '202510230009'.", 3600, { small: true, shading: LIGHT_GRAY }),
      dataCell("SELECT TOP 1 amount FROM WP_M09.dbo.WP_vOutStock WHERE OutStkId = '202510230009' AND isDel = 'N';", 5426, { mono: true, small: true, shading: LIGHT_GRAY }),
    ]}),
  ]
}),

heading3("B. 訓練集 — train_claude_en_2000.json（1,748 筆）範例"),
bodyText("此為 Claude 生成的擴充訓練資料，涵蓋更多語言變化（中英混合）與 View 分布："),
emptyLine(),
new Table({
  width: { size: CONTENT_WIDTH, type: WidthType.DXA },
  columnWidths: [3600, 5426],
  rows: [
    new TableRow({ children: [headerCell("自然語言問句", 3600), headerCell("目標 SQL", 5426)] }),
    new TableRow({ children: [
      dataCell("What is the total purchase amount for 玫瑰花茶?", 3600, { small: true }),
      dataCell("SELECT SUM(amtTotal) AS total FROM WP_M09.dbo.WP_vAcctOut WHERE pName=N'玫瑰花茶' AND isDel='N' AND dtlIsDel='N';", 5426, { mono: true, small: true }),
    ]}),
    new TableRow({ children: [
      dataCell("In which warehouses is 洋甘菊茶 stored?", 3600, { small: true, shading: LIGHT_GRAY }),
      dataCell("SELECT WarehouseName, qty FROM WP_M09.dbo.WP_vInventory WHERE pName=N'洋甘菊茶';", 5426, { mono: true, small: true, shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("What is the average inventory quantity per product line?", 3600, { small: true }),
      dataCell("SELECT AVG(qty) AS avg_qty FROM WP_M09.dbo.WP_vInventory;", 5426, { mono: true, small: true }),
    ]}),
    new TableRow({ children: [
      dataCell("Show products with name containing 茶", 3600, { small: true, shading: LIGHT_GRAY }),
      dataCell("SELECT pNo, pName FROM WP_M09.dbo.WP_vProduct WHERE pName LIKE N'%茶%';", 5426, { mono: true, small: true, shading: LIGHT_GRAY }),
    ]}),
  ]
}),

heading3("C. 手工擴增資料（50 筆）— 針對 5 大弱模式"),
bodyText("根據錯誤分析，針對模型表現最差的 5 個模式，手工設計高品質訓練範例："),
emptyLine(),
new Table({
  width: { size: CONTENT_WIDTH, type: WidthType.DXA },
  columnWidths: [1400, 1200, 6426],
  rows: [
    new TableRow({ children: [headerCell("類別", 1400), headerCell("數量", 1200), headerCell("範例", 6426)] }),
    new TableRow({ children: [
      dataCell("OutStock 辨識", 1400, { bold: true }),
      dataCell("18 筆", 1200, { center: true }),
      multiCell([
        { text: "Q: 2025年12月的銷貨出庫總金額是多少?", size: 18 },
        { text: "SQL: SELECT SUM(amount) FROM (SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE isDel='N' AND LEFT(OutStkId,6)='202512') sub", mono: true, size: 16 },
      ], 6426),
    ]}),
    new TableRow({ children: [
      dataCell("子查詢去重", 1400, { bold: true, shading: LIGHT_GRAY }),
      dataCell("11 筆", 1200, { center: true, shading: LIGHT_GRAY }),
      multiCell([
        { text: "Q: What is the total receivable amount in 2025?", size: 18 },
        { text: "SQL: SELECT SUM(amount) FROM (SELECT DISTINCT acctInId, amount FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND LEFT(acctInId,4)='2025') sub", mono: true, size: 16 },
      ], 6426, { shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("isDel 成對", 1400, { bold: true }),
      dataCell("12 筆", 1200, { center: true }),
      multiCell([
        { text: "同一問句在 isDel-View 與 non-isDel-View 的正確寫法對比", size: 18 },
        { text: "讓模型學會：何時加 isDel='N' 何時不加", size: 18, color: "2E75B6" },
      ], 6426),
    ]}),
    new TableRow({ children: [
      dataCell("DISTINCT", 1400, { bold: true, shading: LIGHT_GRAY }),
      dataCell("5 筆", 1200, { center: true, shading: LIGHT_GRAY }),
      dataCell("「列出所有...」「有哪些...」等需 DISTINCT 的查詢模式", 6426, { small: true, shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("Transfer", 1400, { bold: true }),
      dataCell("4 筆", 1200, { center: true }),
      dataCell("調撥單特殊欄位（fWhName/tWhName, costAvg）查詢", 6426, { small: true }),
    ]}),
  ]
}),

heading3("D. 驗證集 — val_claude_en_spider_v2.json（238 筆）範例"),
bodyText("全新建立的驗證集，每個 View 恰好 34 筆，含 easy/medium/hard 三個難度，全部通過 DB 執行驗證："),
emptyLine(),
new Table({
  width: { size: CONTENT_WIDTH, type: WidthType.DXA },
  columnWidths: [2800, 4626, 800, 802],
  rows: [
    new TableRow({ children: [headerCell("問句", 2800), headerCell("SQL", 4626), headerCell("View", 800), headerCell("難度", 802)] }),
    new TableRow({ children: [
      dataCell("List all active accounts receivable IDs.", 2800, { small: true }),
      dataCell("SELECT DISTINCT acctInId FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' AND dtlIsDel='N'", 4626, { mono: true, small: true }),
      dataCell("AcctIn", 800, { small: true, center: true }),
      dataCell("easy", 802, { small: true, center: true, color: "008000" }),
    ]}),
    new TableRow({ children: [
      dataCell("What is the total outbound stock amount in month 202510?", 2800, { small: true, shading: LIGHT_GRAY }),
      dataCell("SELECT SUM(amount) FROM (SELECT DISTINCT OutStkId, amount FROM WP_M09.dbo.WP_vOutStock WHERE LEFT(OutStkId,6)='202510' AND isDel='N') sub", 4626, { mono: true, small: true, shading: LIGHT_GRAY }),
      dataCell("OutStock", 800, { small: true, center: true, shading: LIGHT_GRAY }),
      dataCell("medium", 802, { small: true, center: true, color: "FF8C00", shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("Which member has the highest total accounts receivable amount?", 2800, { small: true }),
      dataCell("SELECT TOP 1 memName, SUM(amt) AS total FROM (SELECT DISTINCT acctInId, memName, amount AS amt FROM WP_M09.dbo.WP_vAcctIn WHERE isDel='N' ...) sub GROUP BY memName ORDER BY total DESC", 4626, { mono: true, small: true }),
      dataCell("AcctIn", 800, { small: true, center: true }),
      dataCell("hard", 802, { small: true, center: true, color: "CC0000" }),
    ]}),
    new TableRow({ children: [
      dataCell("Which source warehouse has the most transfer records?", 2800, { small: true, shading: LIGHT_GRAY }),
      dataCell("SELECT TOP 1 fWhName, COUNT(DISTINCT TransferId) AS cnt FROM WP_M09.dbo.WP_vTransfer WHERE isDel='N' AND dtlIsDel='N' GROUP BY fWhName ORDER BY cnt DESC", 4626, { mono: true, small: true, shading: LIGHT_GRAY }),
      dataCell("Transfer", 800, { small: true, center: true, shading: LIGHT_GRAY }),
      dataCell("hard", 802, { small: true, center: true, color: "CC0000", shading: LIGHT_GRAY }),
    ]}),
  ]
}),

bodyText("驗證集統計：", { bold: true }),
bulletItem("難度分布：Easy 98 筆 (41.2%) / Medium 84 筆 (35.3%) / Hard 56 筆 (23.5%)"),
bulletItem("子查詢佔比：13.4%（訓練集 Spider 為 5.4%，Claude 為 0%）"),
bulletItem("與訓練集 SQL 重疊率：僅 5%（確保評估有效性）"),

// =====================================================================
// NEW: 1.6 HYPERPARAMETERS WITH EXPLANATIONS
// =====================================================================
new Paragraph({ children: [new PageBreak()] }),
heading2("1.6 超參數配置與意義說明"),
bodyText("Enterprise v0322 的完整超參數配置如下："),
emptyLine(),

new Table({
  width: { size: CONTENT_WIDTH, type: WidthType.DXA },
  columnWidths: [2200, 1600, 5226],
  rows: [
    new TableRow({ children: [headerCell("超參數", 2200), headerCell("設定值", 1600), headerCell("意義與選擇理由", 5226)] }),
    new TableRow({ children: [
      dataCell("Base Model", 2200, { bold: true }),
      dataCell("Llama-3.1-8B-Instruct", 1600, { small: true }),
      dataCell("Meta 的 8B 參數指令微調模型。選擇 Instruct 版是因為它已預訓練指令遵循能力，適合 Chat Template 格式的 Text-to-SQL 任務。8B 參數量在單張消費級 GPU 可載入。", 5226, { small: true }),
    ]}),
    new TableRow({ children: [
      dataCell("微調方法: DoRA", 2200, { bold: true, shading: LIGHT_GRAY }),
      dataCell("r=16, alpha=32", 1600, { small: true, shading: LIGHT_GRAY }),
      dataCell("DoRA (Weight-Decomposed LoRA) 將權重分解為方向與大小兩部分，比標準 LoRA 更穩定。r=16 表示低秩矩陣的秩（rank），alpha=32 控制更新幅度（alpha/r=2x 的學習強度）。", 5226, { small: true, shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("量化: 4-bit (NF4)", 2200, { bold: true }),
      dataCell("BitsAndBytes", 1600, { small: true }),
      dataCell("將模型權重從 16-bit 壓縮至 4-bit（NormalFloat4 格式），記憶體從 ~16GB 降至 ~5GB。搭配 DoRA 只微調少量參數（0.54%），在 16GB VRAM 即可訓練 8B 模型。", 5226, { small: true }),
    ]}),
    new TableRow({ children: [
      dataCell("Learning Rate", 2200, { bold: true, shading: LIGHT_GRAY }),
      dataCell("5e-5 (cosine)", 1600, { small: true, shading: LIGHT_GRAY }),
      dataCell("學習率控制每步更新幅度。5e-5 是 LoRA 微調的常見值（比全量微調的 1e-5 略高，因可訓練參數少）。Cosine schedule 讓學習率平滑衰減至 0，避免後期震盪。", 5226, { small: true, shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("Epochs", 2200, { bold: true }),
      dataCell("6", 1600, { small: true }),
      dataCell("完整遍歷訓練集 6 次。過少（<3）模型學不夠，過多（>10）容易 overfit。在 2,585 筆的資料量下，6 epoch 是經驗上的平衡點。", 5226, { small: true }),
    ]}),
    new TableRow({ children: [
      dataCell("Batch Size", 2200, { bold: true, shading: LIGHT_GRAY }),
      dataCell("2 x 8 = 16", 1600, { small: true, shading: LIGHT_GRAY }),
      dataCell("Per-device batch=2（受 VRAM 限制），Gradient Accumulation=8 步，等效 batch=16。較大的等效 batch 讓梯度估計更穩定，訓練更平滑。", 5226, { small: true, shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("MAX_SEQ_LEN", 2200, { bold: true }),
      dataCell("1280 tokens", 1600, { small: true }),
      dataCell("輸入序列最大長度。全 7 表 Schema 約 937 tokens + 問句/答案約 100-200 tokens，設定 1280 留有安全邊界（約 14% margin）。超過此長度的樣本會被截斷。", 5226, { small: true }),
    ]}),
    new TableRow({ children: [
      dataCell("Warmup Ratio", 2200, { bold: true, shading: LIGHT_GRAY }),
      dataCell("0.03", 1600, { small: true, shading: LIGHT_GRAY }),
      dataCell("訓練初期前 3% 步數逐漸提升學習率（從 0 到 5e-5），防止剛開始時大梯度導致權重劇變。在 972 步中約為前 29 步。", 5226, { small: true, shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("Weight Decay", 2200, { bold: true }),
      dataCell("0.01", 1600, { small: true }),
      dataCell("L2 正則化強度。每步對權重施加微小的衰減，防止 overfitting。0.01 是預設值，在小資料集上有助於泛化。", 5226, { small: true }),
    ]}),
    new TableRow({ children: [
      dataCell("Target Modules", 2200, { bold: true, shading: LIGHT_GRAY }),
      dataCell("q/k/v/o/gate/up/down proj", 1600, { small: true, shading: LIGHT_GRAY }),
      dataCell("DoRA 作用的層。同時微調 Attention（q/k/v/o）和 FFN（gate/up/down）的投影矩陣，覆蓋 Transformer 所有關鍵運算，效果優於只調 q/v。", 5226, { small: true, shading: LIGHT_GRAY }),
    ]}),
  ]
}),

// =====================================================================
// 1.7 CURRENT STATUS
// =====================================================================
heading2("1.7 當前訓練狀態"),
bodyText("Enterprise v0322 版本正在訓練中，整合所有改進的完整版本："),
emptyLine(),
new Table({
  width: { size: CONTENT_WIDTH, type: WidthType.DXA },
  columnWidths: [3013, 6013],
  rows: [
    new TableRow({ children: [headerCell("項目", 3013), headerCell("內容", 6013)] }),
    new TableRow({ children: [
      dataCell("訓練腳本", 3013, { bold: true }),
      dataCell("train__enterprise_v0322.py", 6013),
    ]}),
    new TableRow({ children: [
      dataCell("方法論", 3013, { bold: true, shading: LIGHT_GRAY }),
      dataCell("Spider/BIRD-style：全 7 表 Schema + Business Rules + 弱模式擴增", 6013, { shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("訓練樣本", 3013, { bold: true }),
      dataCell("2,585 筆（1,014 Spider + 1,748 Claude + 50 擴增，去重後）", 6013),
    ]}),
    new TableRow({ children: [
      dataCell("模型", 3013, { bold: true, shading: LIGHT_GRAY }),
      dataCell("Llama-3.1-8B-Instruct + DoRA (r=16, alpha=32, 4-bit NF4)", 6013, { shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("總訓練步數", 3013, { bold: true }),
      dataCell("972 步（2,585 筆 / batch 16 x 6 epochs）", 6013),
    ]}),
    new TableRow({ children: [
      dataCell("訓練進度", 3013, { bold: true, shading: LIGHT_BLUE }),
      dataCell("進行中 ~50%（checkpoint-486 已存，Epoch 3/6），預計 03/23 完成", 6013, { shading: LIGHT_BLUE }),
    ]}),
    new TableRow({ children: [
      dataCell("GPU 使用率", 3013, { bold: true }),
      dataCell("99%（12,034 / 16,303 MiB），穩定運行", 6013),
    ]}),
  ]
}),

// =====================================================================
// PART 2: FUTURE PLAN
// =====================================================================
new Paragraph({ children: [new PageBreak()] }),
heading1("第二部分：未來研究計畫（一個月內）"),
divider(),
bodyText("以下為 2026 年 3 月 23 日至 5 月初的詳細研究時程。所有實驗均在單張 GPU（RTX 5070 Ti 16GB）上執行，每次訓練約需 7-8 小時。"),

heading2("2.1 詳細時程表"),

// ---- Week 1 ----
heading3("Week 1（03/23 - 03/29）：完成主實驗 + 啟動消融實驗"),
numberItem("03/23：Enterprise v0322 訓練完成，執行完整評估（EM + EX + Table Selection）"),
explainBox("為何：主實驗是所有後續分析的基準線（baseline）。需要先確認全 Schema + Rules + Augment 的整體效果，才能設計有意義的消融實驗。評估結果決定論文的核心 claim。"),
numberItem("03/24 - 03/25：分析評估結果，識別殘餘錯誤模式，必要時微調擴增資料"),
explainBox("為何：深入分析錯誤案例，分類 failure pattern（表選錯、isDel 遺漏、子查詢缺失、欄位名錯誤等）。若發現新的系統性錯誤，可在消融實驗前修正，避免浪費 GPU 時間。"),
numberItem("03/26 - 03/27：執行 Ablation Exp 1 — 移除全 Schema（--schema single）"),
explainBox("為何：這是最關鍵的消融實驗。比較「全 7 表 Schema」vs「只給單表 Schema」，直接量化 Spider 方法論（全 Schema 讓模型自行選表）的價值。預期 OutStock 表選擇準確率會大幅下降。"),
numberItem("03/28 - 03/29：執行 Ablation Exp 2 — 移除商業規則（--no-rules）"),
explainBox("為何：驗證 BIRD Evidence 概念的有效性。Rules 告訴模型「isDel 怎麼用、何時要子查詢」等隱性知識。移除後預期 isDel 錯誤率和子查詢缺失率上升。"),

// ---- Week 2 ----
heading3("Week 2（03/30 - 04/05）：完成消融實驗 + 結果分析"),
numberItem("03/30 - 03/31：執行 Ablation Exp 3 — 移除資料擴增（--no-augment）"),
explainBox("為何：驗證「少量精準擴增」策略的價值。50 筆手工擴增在 2,585 筆中僅佔 1.9%，但針對的都是 0% 或 <6% 的弱模式。若移除後這些弱模式指標大幅下降，證明錯誤分析驅動的擴增極具成本效益。"),
numberItem("04/01 - 04/02：執行 Ablation Exp 4 — 無 Schema Baseline（--schema none）"),
explainBox("為何：建立「無任何 Schema 資訊」的下界（lower bound）。模型只靠問句和訓練時記憶的 Schema 知識生成 SQL，預期表現最差。這個數字讓論文能說明 Schema 資訊到底值多少。"),
numberItem("04/03 - 04/05：彙整所有實驗結果，製作比較表格與圖表"),
explainBox("為何：5 組實驗產生大量數據（每組有 EM/EX/Table Selection x 7 Views x 3 Difficulties = 30+ 指標）。需要系統化整理為論文用的表格和圖表，識別最有 insight 的對比。"),

// ---- Week 3 ----
heading3("Week 3（04/06 - 04/12）：錯誤分析 + 論文撰寫（前半）"),
numberItem("04/06 - 04/08：深入錯誤分析 — 分類錯誤類型、找出系統性 failure pattern"),
explainBox("為何：好的論文不只報數字，還要解釋「為什麼對、為什麼錯」。錯誤分析提供定性的深度理解，補充數字上的定量分析，也為 Discussion 章節提供素材。"),
numberItem("04/09 - 04/10：撰寫論文 Chapter 1-3（Introduction, Related Work, Methodology）"),
explainBox("為何：方法論章節在實驗全部完成後撰寫最佳——此時對系統設計的理解最深入，能準確描述每個設計決策的動機。Related Work 需對比 Spider/BIRD/DIN-SQL/DAIL-SQL 等現有方法。"),
numberItem("04/11 - 04/12：撰寫論文 Chapter 4（Experimental Setup）"),
explainBox("為何：實驗設定章節需要精確描述資料集、評估指標、消融配置，確保實驗可重現（reproducibility）。這是學術論文的核心要求。"),

// ---- Week 4 ----
heading3("Week 4（04/13 - 04/19）：論文撰寫（後半）"),
numberItem("04/13 - 04/15：撰寫 Chapter 5（Results & Analysis）含所有表格和圖表"),
explainBox("為何：結果章節是論文最重要的部分。需要呈現：主實驗結果表、消融實驗對比表、per-view 分析圖、per-difficulty 分析、錯誤分類統計。每個表格都需要文字解讀。"),
numberItem("04/16 - 04/17：撰寫 Chapter 6（Discussion）"),
explainBox("為何：討論章節將結果放入更大的脈絡：與 Spider/BIRD 原始結果比較、方法的限制、對業界的啟示。這是展示研究深度和批判思考的關鍵章節。"),
numberItem("04/18 - 04/19：撰寫 Chapter 7（Conclusion）、Abstract"),
explainBox("為何：Abstract 和結論需要在全文完成後撰寫，才能準確概括研究的核心發現。Abstract 決定論文的第一印象。"),

// ---- Week 5 ----
heading3("Week 5（04/20 - 04/26）：論文修訂 + 補充實驗"),
numberItem("04/20 - 04/22：全文修訂第一輪 — 邏輯一致性、數據正確性、表達精確度"),
explainBox("為何：初稿通常有數據前後不一致、論述邏輯跳躍、表達不夠精確等問題。第一輪修訂重點在「內容正確性」而非文字美觀。"),
numberItem("04/23 - 04/24：根據指導教授回饋，進行補充實驗或分析"),
explainBox("為何：預留彈性時間。教授可能要求額外的實驗（如不同 rank、不同 LR）、更深入的某方面分析、或補充特定的 related work。"),
numberItem("04/25 - 04/26：製作 Appendix（完整 Schema、範例 SQL、錯誤案例）"),

// ---- Week 6 ----
heading3("Week 6（04/27 - 05/03）：最終修訂與完稿"),
numberItem("04/27 - 04/29：全文修訂第二輪 — 格式統一、參考文獻完善、圖表美化"),
numberItem("04/30 - 05/01：英文 Abstract 與關鍵字確認"),
numberItem("05/02 - 05/03：最終校對，完稿提交"),

heading2("2.2 消融實驗設計"),
bodyText("共 5 組實驗，量化各技術組件的獨立貢獻："),
emptyLine(),
new Table({
  width: { size: CONTENT_WIDTH, type: WidthType.DXA },
  columnWidths: [1000, 2000, 2826, 3200],
  rows: [
    new TableRow({ children: [headerCell("實驗", 1000), headerCell("配置", 2000), headerCell("CLI 指令", 2826), headerCell("預期驗證", 3200)] }),
    new TableRow({ children: [
      dataCell("Exp 0", 1000, { bold: true, center: true }),
      dataCell("Full (Baseline)", 2000),
      dataCell("python train__enterprise_v0322.py", 2826, { small: true }),
      dataCell("對照組 — 全部技術整合", 3200),
    ]}),
    new TableRow({ children: [
      dataCell("Exp 1", 1000, { bold: true, center: true, shading: LIGHT_GRAY }),
      dataCell("w/o Full Schema", 2000, { shading: LIGHT_GRAY }),
      dataCell("--schema single", 2826, { small: true, shading: LIGHT_GRAY }),
      dataCell("全 Schema 自動選表 vs keyword 選表", 3200, { shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("Exp 2", 1000, { bold: true, center: true }),
      dataCell("w/o Business Rules", 2000),
      dataCell("--no-rules", 2826, { small: true }),
      dataCell("BIRD Evidence 概念的有效性", 3200),
    ]}),
    new TableRow({ children: [
      dataCell("Exp 3", 1000, { bold: true, center: true, shading: LIGHT_GRAY }),
      dataCell("w/o Augmentation", 2000, { shading: LIGHT_GRAY }),
      dataCell("--no-augment", 2826, { small: true, shading: LIGHT_GRAY }),
      dataCell("50 筆精準擴增的成本效益", 3200, { shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("Exp 4", 1000, { bold: true, center: true }),
      dataCell("No Schema", 2000),
      dataCell("--schema none", 2826, { small: true }),
      dataCell("Schema 資訊的必要性（下界）", 3200),
    ]}),
  ]
}),

// =====================================================================
// PART 3: PAPER OUTLINE
// =====================================================================
new Paragraph({ children: [new PageBreak()] }),
heading1("第三部分：研究論文目錄草稿"),
divider(),
bodyText("論文暫定題目：", { bold: true }),
new Paragraph({
  spacing: { after: 80, line: 360 },
  alignment: AlignmentType.CENTER,
  border: { top: { style: BorderStyle.SINGLE, size: 1, color: LIGHT_BLUE, space: 4 }, bottom: { style: BorderStyle.SINGLE, size: 1, color: LIGHT_BLUE, space: 4 } },
  children: [new TextRun({ text: "「基於大型語言模型微調之企業級 Text-to-SQL 落地方法論研究」", font: "Microsoft JhengHei", size: 24, bold: true, color: BLUE })]
}),
new Paragraph({
  spacing: { after: 200, line: 360 },
  alignment: AlignmentType.CENTER,
  children: [new TextRun({ text: "Enterprise Text-to-SQL: A Spider/BIRD-Inspired Methodology for Fine-Tuning LLMs on Production Databases", font: "Arial", size: 22, italics: true, color: MEDIUM_GRAY })]
}),

emptyLine(),
heading2("論文架構"),
emptyLine(),

heading3("第一章 緒論 (Introduction)"),
bodyText("1.1 研究背景與動機"), bodyText("1.2 研究問題"), bodyText("1.3 研究目標與貢獻"), bodyText("1.4 論文架構"),

heading3("第二章 文獻探討 (Related Work)"),
bodyText("2.1 Text-to-SQL Benchmark — Spider 1.0, Spider 2.0, BIRD"),
bodyText("2.2 大型語言模型於 Text-to-SQL — GPT-4, DIN-SQL, DAIL-SQL, C3-SQL"),
bodyText("2.3 參數高效微調 (PEFT) — LoRA, DoRA, QLoRA"),
bodyText("2.4 企業級 Text-to-SQL 挑戰 — Schema 複雜度、領域知識、多語言"),

heading3("第三章 研究方法 (Methodology)"),
bodyText("3.1 系統架構概覽"), bodyText("3.2 資料庫分析與 Schema 設計"),
bodyText("3.3 全 Schema Prompt 設計（Spider 方法論）"), bodyText("3.4 商業規則注入（BIRD Evidence 概念）"),
bodyText("3.5 弱模式資料擴增策略"), bodyText("3.6 模型微調配置"),

heading3("第四章 實驗設計 (Experimental Setup)"),
bodyText("4.1 資料集建構"), bodyText("4.2 評估指標（Table Selection / EM / EX）"),
bodyText("4.3 基線模型與比較方法"), bodyText("4.4 消融實驗設計"), bodyText("4.5 實驗環境"),

heading3("第五章 實驗結果與分析 (Results & Analysis)"),
bodyText("5.1 主實驗結果"), bodyText("5.2 Table Selection Accuracy 分析"),
bodyText("5.3 Per-View 結果分析"), bodyText("5.4 Per-Difficulty 結果分析"),
bodyText("5.5 消融實驗結果"), bodyText("5.6 錯誤分析"),

heading3("第六章 討論 (Discussion)"),
bodyText("6.1 Spider/BIRD 方法論於企業環境的有效性"),
bodyText("6.2 全 Schema vs 單表 Schema 的 trade-off"),
bodyText("6.3 商業規則注入的影響"), bodyText("6.4 弱模式資料擴增的效益"),
bodyText("6.5 研究限制"), bodyText("6.6 對企業實務的啟示"),

heading3("第七章 結論與未來展望 (Conclusion & Future Work)"),
bodyText("7.1 研究總結"), bodyText("7.2 主要貢獻"), bodyText("7.3 未來研究方向"),
emptyLine(),
bodyText("參考文獻 (References)"),
bodyText("附錄 A：WP_M09 完整 Schema"), bodyText("附錄 B：訓練資料範例"),
bodyText("附錄 C：錯誤分析完整案例"), bodyText("附錄 D：消融實驗完整數據"),

// =====================================================================
// PART 4: KEY METRICS
// =====================================================================
new Paragraph({ children: [new PageBreak()] }),
heading1("第四部分：關鍵指標追蹤"),
divider(),

heading2("4.1 歷史評估結果"),
emptyLine(),
new Table({
  width: { size: CONTENT_WIDTH, type: WidthType.DXA },
  columnWidths: [2000, 1300, 1300, 1300, 3126],
  rows: [
    new TableRow({ children: [headerCell("版本", 2000), headerCell("日期", 1300), headerCell("EM", 1300), headerCell("EX", 1300), headerCell("備註", 3126)] }),
    new TableRow({ children: [
      dataCell("v0308 (DoRA)", 2000), dataCell("03/09", 1300, { center: true }),
      dataCell("34.07%", 1300, { center: true }), dataCell("--", 1300, { center: true }),
      dataCell("首次 Spider 格式", 3126),
    ]}),
    new TableRow({ children: [
      dataCell("v0315", 2000, { shading: LIGHT_GRAY }), dataCell("03/15", 1300, { center: true, shading: LIGHT_GRAY }),
      dataCell("67.03%", 1300, { center: true, shading: LIGHT_GRAY }), dataCell("--", 1300, { center: true, shading: LIGHT_GRAY }),
      dataCell("加入 Claude 訓練集", 3126, { shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("v0317 (R1)", 2000), dataCell("03/17", 1300, { center: true }),
      dataCell("91.76%", 1300, { center: true, bold: true }), dataCell("--", 1300, { center: true }),
      dataCell("舊測試集（有洩漏風險）", 3126),
    ]}),
    new TableRow({ children: [
      dataCell("v0320 (v3)", 2000, { shading: LIGHT_GRAY }), dataCell("03/20", 1300, { center: true, shading: LIGHT_GRAY }),
      dataCell("34.03%", 1300, { center: true, shading: LIGHT_GRAY }), dataCell("44.12%", 1300, { center: true, shading: LIGHT_GRAY }),
      dataCell("新驗證集（OutStock EX 5.9%）", 3126, { shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("Enterprise v0322", 2000, { bold: true, shading: LIGHT_BLUE }),
      dataCell("03/23", 1300, { center: true, shading: LIGHT_BLUE }),
      dataCell("待測", 1300, { center: true, shading: LIGHT_BLUE }),
      dataCell("待測", 1300, { center: true, shading: LIGHT_BLUE }),
      dataCell("全 Schema + Rules + Augment", 3126, { shading: LIGHT_BLUE }),
    ]}),
  ]
}),

heading2("4.2 預期改善方向"),
emptyLine(),
new Table({
  width: { size: CONTENT_WIDTH, type: WidthType.DXA },
  columnWidths: [2500, 2263, 2263, 2000],
  rows: [
    new TableRow({ children: [headerCell("指標", 2500), headerCell("v0320 現況", 2263), headerCell("Enterprise 預期", 2263), headerCell("改善機制", 2000)] }),
    new TableRow({ children: [
      dataCell("Table Selection", 2500, { bold: true }),
      dataCell("需 keyword 推斷", 2263, { center: true }),
      dataCell("模型自行選表", 2263, { center: true }),
      dataCell("全 Schema", 2000),
    ]}),
    new TableRow({ children: [
      dataCell("OutStock EX", 2500, { bold: true, shading: LIGHT_GRAY }),
      dataCell("5.9%（31/32 選錯表）", 2263, { center: true, shading: LIGHT_GRAY }),
      dataCell("大幅提升", 2263, { center: true, shading: LIGHT_GRAY }),
      dataCell("全 Schema + 18 筆擴增", 2000, { shading: LIGHT_GRAY }),
    ]}),
    new TableRow({ children: [
      dataCell("子查詢去重", 2500, { bold: true }),
      dataCell("0%", 2263, { center: true }),
      dataCell("顯著提升", 2263, { center: true }),
      dataCell("11 筆擴增 + Rules", 2000),
    ]}),
    new TableRow({ children: [
      dataCell("整體 EX", 2500, { bold: true, shading: LIGHT_GRAY }),
      dataCell("44.12%", 2263, { center: true, shading: LIGHT_GRAY }),
      dataCell("目標 > 60%", 2263, { center: true, shading: LIGHT_GRAY }),
      dataCell("綜合改善", 2000, { shading: LIGHT_GRAY }),
    ]}),
  ]
}),

emptyLine(),
divider(),
new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 200 },
  children: [new TextRun({ text: "--- End of Report ---", font: "Arial", size: 22, italics: true, color: MEDIUM_GRAY })] }),

      ] // end children
    } // end section
  ] // end sections
});

Packer.toBuffer(doc).then(buffer => {
  const outPath = "D:/spider1_training/outputs/研究進度報告_20260322_v2.docx";
  fs.writeFileSync(outPath, buffer);
  console.log(`Document saved: ${outPath} (${(buffer.length / 1024).toFixed(1)} KB)`);
});
