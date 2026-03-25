const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, LevelFormat,
  HeadingLevel, BorderStyle, WidthType, ShadingType,
  PageNumber, PageBreak, TabStopType, TabStopPosition
} = require("docx");

// ============================================================
// Helpers
// ============================================================
const SCRIPTS_DIR = "C:/Users/user/AppData/Roaming/Claude/local-agent-mode-sessions/skills-plugin/e36787af-7f33-4025-a872-c0ed4a2a4935/cae274c6-39f4-4613-9af5-41590e51bf74/skills/docx/scripts/office";

const PAGE_WIDTH = 11906; // A4
const PAGE_HEIGHT = 16838;
const MARGIN = 1440; // 1 inch
const CONTENT_WIDTH = PAGE_WIDTH - 2 * MARGIN; // 9026

const BLUE = "1F4E79";
const LIGHT_BLUE = "D6E4F0";
const DARK_GRAY = "333333";
const MEDIUM_GRAY = "666666";
const LIGHT_GRAY = "F2F2F2";

const border = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const borders = { top: border, bottom: border, left: border, right: border };
const noBorder = { style: BorderStyle.NONE, size: 0 };
const noBorders = { top: noBorder, bottom: noBorder, left: noBorder, right: noBorder };

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
        font: "Microsoft JhengHei",
        size: 20,
        bold: opts.bold || false,
        color: opts.color || DARK_GRAY
      })]
    })]
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

// ============================================================
// Build Document
// ============================================================

const doc = new Document({
  styles: {
    default: {
      document: { run: { font: "Microsoft JhengHei", size: 22 } }
    },
    paragraphStyles: [
      {
        id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 32, bold: true, font: "Microsoft JhengHei", color: BLUE },
        paragraph: { spacing: { before: 360, after: 200 }, outlineLevel: 0 }
      },
      {
        id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 26, bold: true, font: "Microsoft JhengHei", color: BLUE },
        paragraph: { spacing: { before: 280, after: 160 }, outlineLevel: 1 }
      },
      {
        id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { size: 22, bold: true, font: "Microsoft JhengHei", color: "2E75B6" },
        paragraph: { spacing: { before: 200, after: 120 }, outlineLevel: 2 }
      },
    ]
  },
  numbering: {
    config: [
      {
        reference: "bullets",
        levels: [
          { level: 0, format: LevelFormat.BULLET, text: "\u2022", alignment: AlignmentType.LEFT,
            style: { paragraph: { indent: { left: 720, hanging: 360 } } } },
          { level: 1, format: LevelFormat.BULLET, text: "\u25E6", alignment: AlignmentType.LEFT,
            style: { paragraph: { indent: { left: 1440, hanging: 360 } } } },
        ]
      },
      {
        reference: "numbers",
        levels: [
          { level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
            style: { paragraph: { indent: { left: 720, hanging: 360 } } } },
          { level: 1, format: LevelFormat.DECIMAL, text: "%2.", alignment: AlignmentType.LEFT,
            style: { paragraph: { indent: { left: 1440, hanging: 360 } } } },
        ]
      },
      {
        reference: "outline",
        levels: [
          { level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
            style: { paragraph: { indent: { left: 720, hanging: 360 } } } },
        ]
      },
    ]
  },
  sections: [
    // ========== COVER PAGE ==========
    {
      properties: {
        page: {
          size: { width: PAGE_WIDTH, height: PAGE_HEIGHT },
          margin: { top: 2880, right: MARGIN, bottom: MARGIN, left: MARGIN }
        }
      },
      children: [
        new Paragraph({ spacing: { after: 600 }, children: [] }),
        new Paragraph({
          alignment: AlignmentType.CENTER,
          spacing: { after: 200 },
          children: [new TextRun({ text: "研究進度報告", font: "Microsoft JhengHei", size: 52, bold: true, color: BLUE })]
        }),
        new Paragraph({
          alignment: AlignmentType.CENTER,
          spacing: { after: 100 },
          border: { bottom: { style: BorderStyle.SINGLE, size: 8, color: BLUE, space: 8 } },
          children: [new TextRun({ text: "Research Progress Report", font: "Arial", size: 28, color: MEDIUM_GRAY, italics: true })]
        }),
        new Paragraph({ spacing: { after: 600 }, children: [] }),
        new Paragraph({
          alignment: AlignmentType.CENTER,
          spacing: { after: 200 },
          children: [new TextRun({ text: "企業級 Text-to-SQL 落地實驗", font: "Microsoft JhengHei", size: 36, color: DARK_GRAY })]
        }),
        new Paragraph({
          alignment: AlignmentType.CENTER,
          spacing: { after: 100 },
          children: [new TextRun({ text: "基於 Llama-3.1-8B + DoRA 微調之繁體中文自然語言轉 T-SQL 系統", font: "Microsoft JhengHei", size: 22, color: MEDIUM_GRAY })]
        }),
        new Paragraph({ spacing: { after: 600 }, children: [] }),
        new Paragraph({
          alignment: AlignmentType.CENTER,
          spacing: { after: 120 },
          children: [new TextRun({ text: "報告日期：2026 年 3 月 22 日", font: "Microsoft JhengHei", size: 24, color: DARK_GRAY })]
        }),
        new Paragraph({
          alignment: AlignmentType.CENTER,
          spacing: { after: 120 },
          children: [new TextRun({ text: "預計完成日期：2026 年 5 月初", font: "Microsoft JhengHei", size: 24, color: DARK_GRAY })]
        }),
      ]
    },

    // ========== MAIN CONTENT ==========
    {
      properties: {
        page: {
          size: { width: PAGE_WIDTH, height: PAGE_HEIGHT },
          margin: { top: MARGIN, right: MARGIN, bottom: MARGIN, left: MARGIN }
        }
      },
      headers: {
        default: new Header({
          children: [new Paragraph({
            border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: LIGHT_BLUE, space: 4 } },
            tabStops: [{ type: TabStopType.RIGHT, position: TabStopPosition.MAX }],
            children: [
              new TextRun({ text: "企業級 Text-to-SQL 研究進度報告", font: "Microsoft JhengHei", size: 18, color: MEDIUM_GRAY }),
              new TextRun({ text: "\t2026.03.22", font: "Microsoft JhengHei", size: 18, color: MEDIUM_GRAY }),
            ]
          })]
        })
      },
      footers: {
        default: new Footer({
          children: [new Paragraph({
            alignment: AlignmentType.CENTER,
            border: { top: { style: BorderStyle.SINGLE, size: 2, color: LIGHT_BLUE, space: 4 } },
            children: [
              new TextRun({ text: "Page ", font: "Arial", size: 18, color: MEDIUM_GRAY }),
              new TextRun({ children: [PageNumber.CURRENT], font: "Arial", size: 18, color: MEDIUM_GRAY }),
            ]
          })]
        })
      },
      children: [
        // ============ PART 1: RESEARCH PROGRESS ============
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

        // Phase timeline table
        new Table({
          width: { size: CONTENT_WIDTH, type: WidthType.DXA },
          columnWidths: [1800, 2200, 2513, 2513],
          rows: [
            new TableRow({ children: [
              headerCell("階段", 1800), headerCell("時間", 2200), headerCell("方法", 2513), headerCell("最佳結果", 2513),
            ]}),
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
        bulletItem("Prompt 格式演進：純文字 -> Spider 格式 -> Chat Template -> 全 Schema + Rules"),

        heading3("D. 評估框架"),
        bulletItem("建立新版驗證集（238 筆），消除舊測試集的資料洩漏風險"),
        bulletItem("評估指標：Table Selection Accuracy + String EM + Execution Accuracy (EX)"),
        bulletItem("多維度分析：per-view（7 個 View）、per-difficulty（easy/medium/hard）"),
        bulletItem("Enterprise 評估腳本與訓練腳本共用 schema（single source of truth）"),

        heading2("1.4 關鍵發現與問題診斷"),
        emptyLine(),

        // Key findings table
        new Table({
          width: { size: CONTENT_WIDTH, type: WidthType.DXA },
          columnWidths: [2200, 2413, 2413, 2000],
          rows: [
            new TableRow({ children: [
              headerCell("問題", 2200), headerCell("根因", 2413), headerCell("解決方案", 2413), headerCell("狀態", 2000),
            ]}),
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

        heading2("1.5 當前狀態"),
        bodyText("Enterprise v0322 版本正在訓練中，這是整合所有改進的完整版本："),
        emptyLine(),

        new Table({
          width: { size: CONTENT_WIDTH, type: WidthType.DXA },
          columnWidths: [3013, 6013],
          rows: [
            new TableRow({ children: [
              headerCell("項目", 3013), headerCell("內容", 6013),
            ]}),
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
              dataCell("2,585 筆（去重後）", 6013),
            ]}),
            new TableRow({ children: [
              dataCell("模型", 3013, { bold: true, shading: LIGHT_GRAY }),
              dataCell("Llama-3.1-8B-Instruct + DoRA (r=16, alpha=32, 4-bit)", 6013, { shading: LIGHT_GRAY }),
            ]}),
            new TableRow({ children: [
              dataCell("超參數", 3013, { bold: true }),
              dataCell("Epochs=6, LR=5e-5 (cosine), Batch=2x8=16, SEQ_LEN=1280", 6013),
            ]}),
            new TableRow({ children: [
              dataCell("訓練進度", 3013, { bold: true, shading: LIGHT_BLUE }),
              dataCell("進行中 ~22%（Step 222/972, Epoch 1-2/6），預計 03/23 完成", 6013, { shading: LIGHT_BLUE }),
            ]}),
          ]
        }),

        // ============ PAGE BREAK ============
        new Paragraph({ children: [new PageBreak()] }),

        // ============ PART 2: FUTURE PLAN ============
        heading1("第二部分：未來研究計畫（一個月內）"),
        divider(),

        bodyText("以下為 2026 年 3 月 23 日至 5 月初的詳細研究時程規劃。所有實驗均在單張 GPU（RTX 5070 Ti 16GB）上執行，每次訓練約需 7-8 小時。"),

        heading2("2.1 詳細時程表"),

        // ---- Week 1 ----
        heading3("Week 1（03/23 - 03/29）：完成主實驗 + 啟動消融實驗"),
        numberItem("03/23：Enterprise v0322 訓練完成，執行完整評估（EM + EX + Table Selection）"),
        numberItem("03/24 - 03/25：分析評估結果，識別殘餘錯誤模式，必要時微調擴增資料"),
        numberItem("03/26 - 03/27：執行 Ablation Exp 1 — 移除全 Schema（--schema single）"),
        numberItem("03/28 - 03/29：執行 Ablation Exp 2 — 移除商業規則（--no-rules）"),

        // ---- Week 2 ----
        heading3("Week 2（03/30 - 04/05）：完成消融實驗 + 結果分析"),
        numberItem("03/30 - 03/31：執行 Ablation Exp 3 — 移除資料擴增（--no-augment）"),
        numberItem("04/01 - 04/02：執行 Ablation Exp 4 — 無 Schema Baseline（--schema none）"),
        numberItem("04/03 - 04/05：彙整所有實驗結果，製作比較表格與圖表"),

        // ---- Week 3 ----
        heading3("Week 3（04/06 - 04/12）：錯誤分析 + 論文撰寫（前半）"),
        numberItem("04/06 - 04/08：深入錯誤分析 — 分類錯誤類型、找出系統性 failure pattern"),
        numberItem("04/09 - 04/10：撰寫論文 Chapter 1-3（Introduction, Related Work, Methodology）"),
        numberItem("04/11 - 04/12：撰寫論文 Chapter 4（Experimental Setup）"),

        // ---- Week 4 ----
        heading3("Week 4（04/13 - 04/19）：論文撰寫（後半）"),
        numberItem("04/13 - 04/15：撰寫論文 Chapter 5（Results & Analysis），含所有表格和圖表"),
        numberItem("04/16 - 04/17：撰寫論文 Chapter 6（Discussion）— 限制、啟示、與現有方法比較"),
        numberItem("04/18 - 04/19：撰寫論文 Chapter 7（Conclusion）、Abstract"),

        // ---- Week 5 ----
        heading3("Week 5（04/20 - 04/26）：論文修訂 + 補充實驗"),
        numberItem("04/20 - 04/22：全文修訂第一輪 — 邏輯一致性、數據正確性、表達精確度"),
        numberItem("04/23 - 04/24：若有 Reviewer 或指導教授回饋，進行補充實驗或分析"),
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
          columnWidths: [1200, 2400, 3226, 2200],
          rows: [
            new TableRow({ children: [
              headerCell("實驗", 1200), headerCell("配置", 2400), headerCell("CLI 指令", 3226), headerCell("預期驗證", 2200),
            ]}),
            new TableRow({ children: [
              dataCell("Exp 0", 1200, { bold: true, center: true }),
              dataCell("Full (Baseline)", 2400),
              dataCell("python train__enterprise_v0322.py", 3226),
              dataCell("—（對照組）", 2200),
            ]}),
            new TableRow({ children: [
              dataCell("Exp 1", 1200, { bold: true, center: true, shading: LIGHT_GRAY }),
              dataCell("w/o Full Schema", 2400, { shading: LIGHT_GRAY }),
              dataCell("--schema single", 3226, { shading: LIGHT_GRAY }),
              dataCell("全 Schema 的貢獻", 2200, { shading: LIGHT_GRAY }),
            ]}),
            new TableRow({ children: [
              dataCell("Exp 2", 1200, { bold: true, center: true }),
              dataCell("w/o Business Rules", 2400),
              dataCell("--no-rules", 3226),
              dataCell("商業規則的貢獻", 2200),
            ]}),
            new TableRow({ children: [
              dataCell("Exp 3", 1200, { bold: true, center: true, shading: LIGHT_GRAY }),
              dataCell("w/o Augmentation", 2400, { shading: LIGHT_GRAY }),
              dataCell("--no-augment", 3226, { shading: LIGHT_GRAY }),
              dataCell("弱模式擴增的貢獻", 2200, { shading: LIGHT_GRAY }),
            ]}),
            new TableRow({ children: [
              dataCell("Exp 4", 1200, { bold: true, center: true }),
              dataCell("No Schema", 2400),
              dataCell("--schema none", 3226),
              dataCell("Schema 資訊的必要性", 2200),
            ]}),
          ]
        }),

        heading2("2.3 預期產出"),
        bulletItem("完整的主實驗結果（Enterprise v0322 EM/EX/Table Selection，per-view, per-difficulty）"),
        bulletItem("4 組消融實驗結果，量化全 Schema / 商業規則 / 資料擴增各自的獨立貢獻"),
        bulletItem("詳細錯誤分析報告（分類、分布、代表性案例）"),
        bulletItem("完整學術論文（預計 5 月初完稿）"),

        // ============ PAGE BREAK ============
        new Paragraph({ children: [new PageBreak()] }),

        // ============ PART 3: PAPER OUTLINE ============
        heading1("第三部分：研究論文目錄草稿"),
        divider(),

        bodyText("論文暫定題目：", { bold: true }),
        new Paragraph({
          spacing: { after: 80, line: 360 },
          alignment: AlignmentType.CENTER,
          border: { top: { style: BorderStyle.SINGLE, size: 1, color: LIGHT_BLUE, space: 4 }, bottom: { style: BorderStyle.SINGLE, size: 1, color: LIGHT_BLUE, space: 4 } },
          children: [
            new TextRun({ text: "「基於大型語言模型微調之企業級 Text-to-SQL 落地方法論研究」", font: "Microsoft JhengHei", size: 24, bold: true, color: BLUE }),
          ]
        }),
        new Paragraph({
          spacing: { after: 200, line: 360 },
          alignment: AlignmentType.CENTER,
          children: [
            new TextRun({ text: "Enterprise Text-to-SQL: A Spider/BIRD-Inspired Methodology", font: "Arial", size: 22, italics: true, color: MEDIUM_GRAY }),
          ]
        }),
        new Paragraph({
          spacing: { after: 60, line: 360 },
          alignment: AlignmentType.CENTER,
          children: [
            new TextRun({ text: "for Fine-Tuning LLMs on Production Databases", font: "Arial", size: 22, italics: true, color: MEDIUM_GRAY }),
          ]
        }),

        emptyLine(),
        heading2("論文架構"),
        emptyLine(),

        // Chapter 1
        heading3("第一章　緒論 (Introduction)"),
        bodyText("1.1 研究背景與動機"),
        bodyText("    — Text-to-SQL 技術現況、企業資料庫的特殊挑戰", { indent: 360 }),
        bodyText("1.2 研究問題"),
        bodyText("    — Spider/BIRD 方法論能否直接應用於企業環境？主要障礙為何？", { indent: 360 }),
        bodyText("1.3 研究目標與貢獻"),
        bodyText("    — 提出企業級 Text-to-SQL 落地方法論、驗證效度、量化各技術貢獻", { indent: 360 }),
        bodyText("1.4 論文架構"),

        // Chapter 2
        heading3("第二章　文獻探討 (Related Work)"),
        bodyText("2.1 Text-to-SQL Benchmark"),
        bodyText("    — Spider 1.0 (Yu et al., 2018)、Spider 2.0、BIRD (Li et al., 2024)", { indent: 360 }),
        bodyText("2.2 大型語言模型於 Text-to-SQL"),
        bodyText("    — GPT-4、CodeLlama、DIN-SQL、DAIL-SQL、C3-SQL", { indent: 360 }),
        bodyText("2.3 參數高效微調 (PEFT)"),
        bodyText("    — LoRA (Hu et al., 2021)、DoRA (Liu et al., 2024)、QLoRA", { indent: 360 }),
        bodyText("2.4 企業級 Text-to-SQL 挑戰"),
        bodyText("    — Schema 複雜度、領域知識、多語言、軟刪除等生產環境特殊需求", { indent: 360 }),

        // Chapter 3
        heading3("第三章　研究方法 (Methodology)"),
        bodyText("3.1 系統架構概覽"),
        bodyText("    — 整體 pipeline：NL -> Prompt 建構 -> LLM 推論 -> SQL 輸出", { indent: 360 }),
        bodyText("3.2 資料庫分析與 Schema 設計"),
        bodyText("    — WP_M09 七個 View 結構、Header-Detail 關聯、商業邏輯歸納", { indent: 360 }),
        bodyText("3.3 全 Schema Prompt 設計（Spider 方法論）"),
        bodyText("    — 緊湊格式 vs CREATE TABLE、Token 長度分析與壓縮策略", { indent: 360 }),
        bodyText("3.4 商業規則注入（BIRD Evidence 概念）"),
        bodyText("    — isDel 過濾規則、子查詢去重規則、日期篩選規則", { indent: 360 }),
        bodyText("3.5 弱模式資料擴增策略"),
        bodyText("    — 錯誤分析驅動的針對性擴增、5 類弱模式定義與範例設計", { indent: 360 }),
        bodyText("3.6 模型微調配置"),
        bodyText("    — Llama-3.1-8B-Instruct、DoRA、4-bit 量化、Chat Template", { indent: 360 }),

        // Chapter 4
        heading3("第四章　實驗設計 (Experimental Setup)"),
        bodyText("4.1 資料集"),
        bodyText("    — 訓練集建構過程（3 個來源, 2,585 筆）、驗證集設計（238 筆）", { indent: 360 }),
        bodyText("4.2 評估指標"),
        bodyText("    — Table Selection Accuracy、String Exact Match (EM)、Execution Accuracy (EX)", { indent: 360 }),
        bodyText("4.3 基線模型與比較方法"),
        bodyText("    — 單表 Schema 版本（v0317, v0322）、無 Schema baseline", { indent: 360 }),
        bodyText("4.4 消融實驗設計"),
        bodyText("    — Full Schema / Business Rules / Augmentation 各自的獨立消融", { indent: 360 }),
        bodyText("4.5 實驗環境"),
        bodyText("    — 硬體配置、訓練時間、可重現性", { indent: 360 }),

        // Chapter 5
        heading3("第五章　實驗結果與分析 (Results & Analysis)"),
        bodyText("5.1 主實驗結果"),
        bodyText("    — Enterprise v0322 整體 EM/EX、與先前版本比較", { indent: 360 }),
        bodyText("5.2 Table Selection Accuracy 分析"),
        bodyText("    — 全 Schema vs 單表 Schema 的表選擇準確率對比", { indent: 360 }),
        bodyText("5.3 Per-View 結果分析"),
        bodyText("    — 7 個 View 各自的表現、OutStock 改善幅度", { indent: 360 }),
        bodyText("5.4 Per-Difficulty 結果分析"),
        bodyText("    — Easy/Medium/Hard 的表現差異與難點所在", { indent: 360 }),
        bodyText("5.5 消融實驗結果"),
        bodyText("    — 各組件移除後的表現下降幅度、貢獻量化", { indent: 360 }),
        bodyText("5.6 錯誤分析"),
        bodyText("    — 殘餘錯誤分類、典型案例、系統性 failure pattern", { indent: 360 }),

        // Chapter 6
        heading3("第六章　討論 (Discussion)"),
        bodyText("6.1 Spider/BIRD 方法論於企業環境的有效性"),
        bodyText("6.2 全 Schema vs 單表 Schema 的 trade-off"),
        bodyText("    — Token 效率、選表能力、推論延遲", { indent: 360 }),
        bodyText("6.3 商業規則注入的影響"),
        bodyText("    — 類比 BIRD evidence、隱性 vs 顯性知識注入", { indent: 360 }),
        bodyText("6.4 弱模式資料擴增的效益"),
        bodyText("    — 少量精準擴增 vs 大量泛化擴增", { indent: 360 }),
        bodyText("6.5 研究限制"),
        bodyText("    — 單資料庫、有限表數量、特定 SQL 方言、硬體限制", { indent: 360 }),
        bodyText("6.6 對企業實務的啟示"),
        bodyText("    — 部署建議、持續改善策略、成本效益分析", { indent: 360 }),

        // Chapter 7
        heading3("第七章　結論與未來展望 (Conclusion & Future Work)"),
        bodyText("7.1 研究總結"),
        bodyText("7.2 主要貢獻"),
        bodyText("7.3 未來研究方向"),
        bodyText("    — 多資料庫泛化、RAG 增強、使用者回饋學習、更大模型探索", { indent: 360 }),

        emptyLine(),
        bodyText("參考文獻 (References)"),
        bodyText("附錄 A：WP_M09 完整 Schema"),
        bodyText("附錄 B：訓練資料範例"),
        bodyText("附錄 C：錯誤分析完整案例"),
        bodyText("附錄 D：消融實驗完整數據"),

        // ============ PAGE BREAK ============
        new Paragraph({ children: [new PageBreak()] }),

        // ============ PART 4: KEY METRICS ============
        heading1("第四部分：關鍵指標追蹤"),
        divider(),

        heading2("4.1 歷史評估結果"),
        emptyLine(),

        // Results table
        new Table({
          width: { size: CONTENT_WIDTH, type: WidthType.DXA },
          columnWidths: [2000, 1300, 1300, 1300, 3126],
          rows: [
            new TableRow({ children: [
              headerCell("版本", 2000), headerCell("日期", 1300), headerCell("EM", 1300), headerCell("EX", 1300), headerCell("備註", 3126),
            ]}),
            new TableRow({ children: [
              dataCell("v0308 (DoRA)", 2000), dataCell("03/09", 1300, { center: true }),
              dataCell("34.07%", 1300, { center: true }), dataCell("—", 1300, { center: true }),
              dataCell("首次 Spider 格式", 3126),
            ]}),
            new TableRow({ children: [
              dataCell("v0315", 2000, { shading: LIGHT_GRAY }), dataCell("03/15", 1300, { center: true, shading: LIGHT_GRAY }),
              dataCell("67.03%", 1300, { center: true, shading: LIGHT_GRAY }), dataCell("—", 1300, { center: true, shading: LIGHT_GRAY }),
              dataCell("加入 Claude 訓練集", 3126, { shading: LIGHT_GRAY }),
            ]}),
            new TableRow({ children: [
              dataCell("v0317 (R1)", 2000), dataCell("03/17", 1300, { center: true }),
              dataCell("91.76%", 1300, { center: true, bold: true }), dataCell("—", 1300, { center: true }),
              dataCell("舊測試集（有洩漏風險）", 3126),
            ]}),
            new TableRow({ children: [
              dataCell("v0320 (v3)", 2000, { shading: LIGHT_GRAY }), dataCell("03/20", 1300, { center: true, shading: LIGHT_GRAY }),
              dataCell("34.03%", 1300, { center: true, shading: LIGHT_GRAY }), dataCell("44.12%", 1300, { center: true, shading: LIGHT_GRAY }),
              dataCell("新驗證集（OutStock 5.9%）", 3126, { shading: LIGHT_GRAY }),
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
            new TableRow({ children: [
              headerCell("指標", 2500), headerCell("v0320 (v3) 現況", 2263), headerCell("Enterprise 預期", 2263), headerCell("改善機制", 2000),
            ]}),
            new TableRow({ children: [
              dataCell("Table Selection", 2500, { bold: true }),
              dataCell("需 keyword 推斷", 2263, { center: true }),
              dataCell("模型自行選表", 2263, { center: true }),
              dataCell("全 Schema", 2000),
            ]}),
            new TableRow({ children: [
              dataCell("OutStock EX", 2500, { bold: true, shading: LIGHT_GRAY }),
              dataCell("5.9%", 2263, { center: true, shading: LIGHT_GRAY }),
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
        new Paragraph({
          alignment: AlignmentType.CENTER,
          spacing: { before: 200 },
          children: [new TextRun({ text: "— End of Report —", font: "Arial", size: 22, italics: true, color: MEDIUM_GRAY })]
        }),
      ]
    }
  ]
});

// ============================================================
// Generate
// ============================================================
Packer.toBuffer(doc).then(buffer => {
  const outPath = "D:/spider1_training/outputs/研究進度報告_20260322.docx";
  fs.writeFileSync(outPath, buffer);
  console.log(`Document saved: ${outPath} (${(buffer.length / 1024).toFixed(1)} KB)`);
});
