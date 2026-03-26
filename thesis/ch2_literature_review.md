# 第二章 文獻探討

本章針對本研究所涉及的核心技術與相關研究進行文獻回顧。首先介紹 Text-to-SQL 任務的定義與發展脈絡（2.1），接續探討大型語言模型的架構演進與 Llama 系列模型（2.2），再深入介紹參數高效微調方法 LoRA 與 DoRA（2.3），以及模型量化技術（2.4）。最後回顧 Text-to-SQL 的評估方法與基準資料集（2.5），並探討中文自然語言處理在 Text-to-SQL 領域的應用現況（2.6）。

---

## 2.1 Text-to-SQL 任務概述

### 2.1.1 任務定義

Text-to-SQL 是自然語言處理（Natural Language Processing, NLP）領域中的一項重要任務，其目標為將使用者以自然語言表達的查詢意圖，自動轉換為可在關聯式資料庫上執行的結構化查詢語言（Structured Query Language, SQL）語句（Androutsopoulos et al., 1995）。此任務可形式化定義為：給定一個自然語言問句 $Q$ 與資料庫綱要（Schema）$S$，模型需生成對應的 SQL 查詢 $Y$，使得 $Y$ 在資料庫上執行後能正確回答問句 $Q$ 所表達的資訊需求。

Text-to-SQL 的研究動機在於降低資料庫查詢的技術門檻，使不具備 資料庫語法的使用者也能透過自然語言與資料庫互動，從而提升企業資料的可及性與應用價值。

### 2.1.2 發展歷程

Text-to-SQL 的研究可追溯至 1970 年代的自然語言介面系統（Natural Language Interface to Databases, NLIDB），如 LUNAR（Woods, 1973）和 CHAT-80（Warren & Pereira, 1982）。早期方法主要依賴手工撰寫的規則與語法解析器，適用範圍受限於特定領域。

隨著深度學習的興起，基於序列到序列（Sequence-to-Sequence, Seq2Seq）架構的方法逐漸成為主流。Zhong et al.（2017）提出 Seq2SQL，首次在大規模 WikiSQL 資料集上展示深度學習方法的可行性。Yu et al.（2018）進一步提出跨資料庫的 Spider 基準資料集，推動了更具通用性的 Text-to-SQL 研究。

近年來，預訓練語言模型（Pre-trained Language Models, PLMs）的應用為 Text-to-SQL 帶來顯著突破。BRIDGE（Lin et al., 2020）利用 BERT 編碼器融合自然語言與資料庫綱要資訊；RESDSQL（Li et al., 2023a）採用 T5 編碼器-解碼器架構，透過排序增強的綱要解耦策略（Ranking-Enhanced Schema Decoupling）將綱要連結與 SQL 生成分離處理；PICARD（Scholak et al., 2021）則透過約束解碼策略確保生成的 SQL 語法正確性。

隨著大型語言模型（Large Language Models, LLMs）如 GPT-4（OpenAI, 2023）、Codex（Chen et al., 2021）的出現，透過提示工程（Prompt Engineering）和上下文學習（In-Context Learning, ICL）進行 Text-to-SQL 的方法也展現了優異表現（Rajkumar et al., 2022; Liu et al., 2023）。2023 年後，基於 LLM 的多階段管線方法（Pipeline Methods）成為新的研究趨勢。DIN-SQL（Pourreza & Rafiei, 2024）將 Text-to-SQL 任務分解為綱要連結、查詢分類、SQL 生成與自我修正四個子任務，以任務分解策略大幅提升了 GPT-4 在 Spider 上的表現。DAIL-SQL（Gao et al., 2024）則系統性地研究了提示工程策略對 LLM Text-to-SQL 性能的影響，提出基於問題相似度與查詢相似度的示例選擇方法。CHESS（Talaei et al., 2024）進一步引入上下文檢索增強的多代理框架，在 BIRD 基準上取得了領先成績。

### 2.1.3 主要挑戰

儘管 Text-to-SQL 研究已取得長足進展，仍面臨多項挑戰：

1. **架構連結**（Schema Linking）：將自然語言問句中的實體與資料庫綱要中的表名、欄位名進行對應，是 Text-to-SQL 的核心子問題。Lei et al.（2020）指出，綱要連結的品質直接影響後續 SQL 生成的準確率。在企業資料庫中，欄位命名常使用縮寫或內部代碼（如本研究中的 `acctInId`、`pvSn`），增加了連結的困難度。
2. **跨資料庫泛化**（Cross-database Generalization）：模型需能處理訓練時未見過的資料庫綱要，這要求模型具備對綱要結構的通用理解能力。
3. **複雜查詢生成**：涉及多表聯結（JOIN）、巢狀子查詢（Nested Subquery）、聚合函數（Aggregation）等複雜 SQL 結構的生成仍具挑戰性。
4. **自然語言歧義**：同一問句可能對應多種合理的 SQL 表示，如何處理語義歧義是關鍵問題。
5. **領域特定知識**：企業應用場景中常涉及特定的業務邏輯與命名慣例，例如本研究中「isDel='N'」用於篩選未刪除記錄的業務規則，此類隱性知識難以僅從綱要結構推斷。BIRD 基準（Li et al., 2024）透過引入外部知識提示來解決此問題，而本研究則透過微調將業務規則直接嵌入模型參數中。
6. **多語言支援**：多數研究以英文為主，對於繁體中文等非英語語言的支援仍有不足。

---

## 2.2 大型語言模型

### 2.2.1 Transformer 架構

Transformer 架構由 Vaswani et al.（2017）於論文《Attention Is All You Need》中提出，以自注意力機制（Self-Attention Mechanism）取代傳統的循環神經網路（Recurrent Neural Network, RNN），實現了對序列資料的高效平行處理。Transformer 由編碼器（Encoder）與解碼器（Decoder）組成，其中自注意力機制透過計算查詢（Query）、鍵（Key）與值（Value）三者之間的注意力權重，使模型能捕捉序列中任意位置之間的依賴關係。

自注意力機制的計算公式如下：

$$\text{Attention}(Q, K, V) = \text{softmax}\left(\frac{QK^T}{\sqrt{d_k}}\right)V$$

其中 $d_k$ 為鍵向量的維度，用於縮放點積以避免梯度消失問題。Transformer 還引入多頭注意力（Multi-Head Attention）機制，將注意力計算分散至多個子空間，增強模型的表達能力。

### 2.2.2 大型語言模型的發展

基於 Transformer 架構，大型語言模型沿著兩條主要路線發展：

**編碼器模型**（Encoder-only）：以 BERT（Devlin et al., 2019）為代表，透過遮蔽語言模型（Masked Language Modeling, MLM）任務進行預訓練，擅長自然語言理解任務。後續衍生出 RoBERTa（Liu et al., 2019）、ALBERT（Lan et al., 2020）等改進版本。

**編碼器-解碼器模型**（Encoder-Decoder）：以 T5（Raffel et al., 2020）和 BART（Lewis et al., 2020）為代表，同時具備理解與生成能力。T5 將所有 NLP 任務統一為「文本到文本」的格式，在 Text-to-SQL 領域曾被廣泛採用作為基礎模型，如 RESDSQL（Li et al., 2023a）和 PICARD（Scholak et al., 2021）。

**解碼器模型**（Decoder-only）：以 GPT 系列（Radford et al., 2018, 2019; Brown et al., 2020）為代表，採用自回歸（Autoregressive）方式進行文本生成。GPT-3（Brown et al., 2020）展示了大規模語言模型的少樣本學習（Few-shot Learning）能力，GPT-4（OpenAI, 2023）則進一步提升了推理與程式碼生成能力。隨著模型規模的持續擴大，解碼器模型在 Text-to-SQL 任務上逐漸超越編碼器-解碼器架構，成為當前的主流方案。

開源社群方面，Meta 推出的 LLaMA 系列（Touvron et al., 2023a, 2023b）在相對較小的參數量下展現了與閉源模型相當的性能，促進了大型語言模型的普及應用。

### 2.2.3 Llama-3.1-8B-Instruct

Llama 3.1（Meta, 2024）是 Meta 推出的第三代開源大型語言模型系列，提供 8B、70B、405B 三種參數規模。相較於前代 Llama 2，Llama 3.1 在以下方面進行了改進：

1. **擴展的訓練資料**：使用超過 15 兆 tokens 的多語言語料進行預訓練，涵蓋英文、中文、日文等多種語言。
2. **更長的上下文窗口**：支援最長 128K tokens 的上下文長度，有利於處理包含完整資料庫綱要的 Text-to-SQL 提示。
3. **分組查詢注意力**（Grouped Query Attention, GQA）：透過共享鍵值頭（Key-Value Head）減少記憶體用量與推論延遲。
4. **指令微調版本**（Instruct）：經過監督式微調（Supervised Fine-Tuning, SFT）與人類回饋強化學習（Reinforcement Learning from Human Feedback, RLHF），使模型能更好地遵循指令與產生結構化輸出。

在分詞器方面，Llama 3.1 採用基於 BPE（Byte Pair Encoding）的分詞器，詞表規模擴展至 128,256 個 tokens，相較於 Llama 2 的 32,000 個 tokens 大幅提升。更大的詞表有助於提高非英語語言的分詞效率，減少繁體中文文本被過度拆分為子詞的問題，從而在有限的上下文窗口中容納更多有效資訊。

本研究選用 Llama-3.1-8B-Instruct 作為基礎模型，主要基於以下考量：（1）8B 參數規模在單張消費級 GPU 上即可進行微調與推論；（2）Instruct 版本已具備遵循指令的能力，適合作為 Text-to-SQL 微調的起點；（3）開源授權允許商業應用與研究使用。至於以中文為核心的 Qwen 系列（Bai et al., 2023）與 Yi 系列（01.AI, 2024），雖在中文理解上具備優勢，但本研究優先驗證以英文為主的通用模型經微調後能否有效處理繁體中文 Text-to-SQL 任務，中文專用模型的比較列為未來研究方向。

---

## 2.3 參數高效微調方法

### 2.3.1 微調的挑戰

全參數微調（Full Fine-Tuning）需要更新模型的所有參數，對於具有數十億參數的大型語言模型而言，所需的 GPU 記憶體與計算資源極為龐大。以 Llama-3.1-8B 為例，全參數微調在 FP16 精度下約需 32 GB 以上的 GPU 記憶體，且需儲存與原始模型等量的梯度與優化器狀態。此外，全參數微調容易導致災難性遺忘（Catastrophic Forgetting），使模型失去預訓練階段所學的通用知識（Kirkpatrick et al., 2017）。

參數高效微調（Parameter-Efficient Fine-Tuning, PEFT）方法旨在僅更新模型的少量參數即可達到與全參數微調相近的效能，同時大幅降低計算與記憶體需求。常見的 PEFT 方法包括 Adapter（Houlsby et al., 2019）、Prefix Tuning（Li & Liang, 2021）、Prompt Tuning（Lester et al., 2021）以及 LoRA（Hu et al., 2022）等。

### 2.3.2 LoRA（Low-Rank Adaptation）

LoRA（Low-Rank Adaptation of Large Language Models）由 Hu et al.（2022）提出，是目前最廣泛使用的 PEFT 方法之一。其核心思想基於一個假設：微調過程中的權重更新矩陣具有低秩（Low-Rank）特性，因此可以用兩個低秩矩陣的乘積來近似。

具體而言，對於預訓練權重矩陣 $W_0 \in \mathbb{R}^{d \times k}$，LoRA 將權重更新表示為：

$$W = W_0 + \Delta W = W_0 + BA$$

其中 $B \in \mathbb{R}^{d \times r}$、$A \in \mathbb{R}^{r \times k}$，$r \ll \min(d, k)$ 為秩（Rank）。在初始化策略上，$A$ 以高斯隨機分布初始化，$B$ 以零矩陣初始化，確保訓練開始時 $\Delta W = BA = 0$，使模型從預訓練權重出發進行漸進式調整。訓練時凍結原始權重 $W_0$，僅更新 $A$ 和 $B$，並以縮放因子 $\alpha/r$ 控制更新幅度。

LoRA 的主要優勢包括：

1. **記憶體高效**：可訓練參數量僅為原模型的 0.1%–1%，大幅減少 GPU 記憶體需求。
2. **無額外推論延遲**：訓練完成後可將 $\Delta W$ 合併至原始權重，推論時不增加計算量。
3. **模組化設計**：可針對不同任務訓練不同的 LoRA 權重，並於推論時靈活切換。
4. **保留預訓練知識**：凍結原始權重有助於減緩災難性遺忘。

在實務應用中，LoRA 通常應用於 Transformer 架構中的注意力層（$W_Q$、$W_K$、$W_V$、$W_O$）與前饋網路層（$W_{gate}$、$W_{up}$、$W_{down}$），以最大化微調效果。

### 2.3.3 DoRA（Weight-Decomposed Low-Rank Adaptation）

DoRA（Weight-Decomposed Low-Rank Adaptation）由 Liu et al.（2024）提出，是 LoRA 的進階改良版本。DoRA 的核心創新在於將權重矩陣分解為方向分量（Directional Component）與大小分量（Magnitude Component），分別進行學習：

$$W = m \cdot \frac{W_0 + BA}{\|W_0 + BA\|_c}$$

其中 $m \in \mathbb{R}^{1 \times k}$ 為可學習的大小向量，$\|\cdot\|_c$ 表示沿列方向的向量範數。透過此分解，LoRA 部分（$BA$）主要負責學習方向變化，而 $m$ 則學習大小調整。

DoRA 相較於 LoRA 的改進包括：

1. **更接近全參數微調的學習行為**：Liu et al.（2024）透過分析發現，全參數微調傾向於對權重的方向與大小進行不同程度的調整，而標準 LoRA 將兩者耦合在一起。DoRA 的分解策略更忠實地模擬了全參數微調的行為模式。
2. **更高的微調品質**：在相同秩 $r$ 的條件下，DoRA 在多項下游任務上的表現優於 LoRA，尤其在需要精細調整的任務中（如程式碼生成與結構化輸出）更為明顯。
3. **穩定的訓練過程**：方向與大小的分離有助於梯度的穩定傳播，降低訓練過程中的振盪現象。

本研究採用 DoRA 作為微調方法，設定秩 $r=8$、縮放因子 $\alpha=16$（$\alpha/r=2.0$），並將其應用於注意力層的全部投影矩陣（$W_Q$、$W_K$、$W_V$、$W_O$）及前饋網路的閘控與投影矩陣（$W_{gate}$、$W_{up}$、$W_{down}$），共計 7 組目標模組。

### 2.3.4 QLoRA 與 DoRA 的協同運作

本研究實際採用的是 QLoRA 與 DoRA 的組合策略。具體而言，基礎模型的預訓練權重 $W_0$ 以 NF4 格式量化至 4 位元（詳見 2.4.2 節），大幅降低記憶體佔用；在此量化基礎上，DoRA 的低秩更新矩陣 $BA$ 與大小向量 $m$ 則以 BF16 精度進行訓練。前向傳播時，量化的基礎權重被反量化（Dequantize）至 BF16 精度，與 DoRA 的更新量合併後進行計算。此設計使得模型僅需約 16 GB GPU 記憶體即可完成微調，同時保留 DoRA 方向-大小分解帶來的微調品質優勢。值得注意的是，由於量化引入的精度損失主要影響基礎權重的大小分量，而 DoRA 的大小向量 $m$ 以全精度學習，恰好能補償此損失，使 DoRA 與量化技術的結合較 LoRA 更具優勢。

---

## 2.4 模型量化技術

### 2.4.1 量化的動機與原理

模型量化（Model Quantization）是一種模型壓縮技術，透過降低模型權重與激活值的數值精度（例如從 32 位浮點數降至 8 位或 4 位整數），以減少模型的記憶體佔用與推論延遲。對於大型語言模型而言，量化技術是在有限硬體資源上部署的關鍵手段。

常見的量化方法可分為兩大類：

1. **訓練後量化**（Post-Training Quantization, PTQ）：在模型訓練完成後進行量化，無需重新訓練。代表方法包括 GPTQ（Frantar et al., 2023）與 AWQ（Lin et al., 2024）。
2. **量化感知訓練**（Quantization-Aware Training, QAT）：在訓練過程中模擬量化效果，使模型學習適應低精度表示。

### 2.4.2 QLoRA 與 NF4 量化

QLoRA（Quantized LoRA）由 Dettmers et al.（2023）提出，結合了 4 位元量化與 LoRA 微調，使得在單張消費級 GPU 上微調大型語言模型成為可能。QLoRA 引入了以下關鍵創新：

**NF4（Normal Float 4-bit）量化**：一種資訊理論上最佳的 4 位元資料類型，專為正態分布的神經網路權重設計。NF4 透過對標準正態分布進行均等面積量化（Equal-area Quantization），使每個量化區間涵蓋相同的概率質量，從而在 4 位元精度下保留最多的資訊。

**雙重量化**（Double Quantization）：對量化常數（Quantization Constants）本身再進行一次量化，進一步減少記憶體佔用。此技術可將每個參數的平均記憶體用量從 4.5 位元降至約 4.125 位元。

**分頁優化器**（Paged Optimizers）：利用 NVIDIA 統一記憶體（Unified Memory）功能，在 GPU 記憶體不足時自動將優化器狀態卸載至 CPU 記憶體，避免因記憶體溢出導致訓練中斷。

本研究採用 QLoRA 的量化策略，以 NF4 格式將 Llama-3.1-8B-Instruct 的基礎權重量化至 4 位元，並啟用雙重量化，計算精度設為 bfloat16。此配置使得模型在約 16 GB GPU 記憶體（NVIDIA RTX 5070 Ti）上即可完成微調訓練。

### 2.4.3 混合精度訓練

混合精度訓練（Mixed Precision Training）由 Micikevicius et al.（2018）提出，透過在訓練過程中同時使用 FP16/BF16 與 FP32 兩種精度，在維持數值穩定性的前提下加速計算並減少記憶體用量。

**BF16（Brain Floating Point 16）**：由 Google 提出，保留與 FP32 相同的 8 位指數位元，犧牲尾數精度以換取更大的動態範圍。相較於 FP16，BF16 在訓練大型語言模型時更不容易出現數值溢出，已成為 LLM 訓練的主流精度格式。

**TF32（TensorFloat-32）**：NVIDIA Ampere 及後續架構引入的計算格式，在矩陣乘法中使用 19 位元精度，兼顧速度與精度。

本研究同時啟用 BF16 混合精度訓練與 TF32 張量核心加速，以在 RTX 5070 Ti 上獲得最佳的訓練效能。

---

## 2.5 Text-to-SQL 評估方法與基準資料集

### 2.5.1 評估指標

Text-to-SQL 任務的評估指標主要分為兩類：

**精確匹配準確率**（Exact Match Accuracy, EM）：比較模型預測的 SQL 查詢與標準答案（Gold SQL）在經過正規化處理後是否完全相同。正規化步驟通常包括：移除末尾分號、統一關鍵字大小寫、正規化空白字元、移除資料庫前綴等。EM 是最嚴格的評估指標，要求預測結果在結構上與標準答案完全一致。

**執行準確率**（Execution Accuracy, EX）：比較模型預測的 SQL 查詢與標準答案在實際資料庫上執行後所返回的結果集是否一致。EX 相較於 EM 更為寬容，因為同一查詢意圖可能存在多種語法不同但結果等價的 SQL 表示。然而，EX 的準確性依賴於資料庫的實際資料內容——若資料庫為空或資料分布過於偏斜，可能產生假陽性（False Positive）。

此外，Yu et al.（2018）在 Spider 基準中引入了**組件匹配**（Component Matching）評估，將 SQL 查詢拆解為 SELECT、WHERE、GROUP BY、ORDER BY、HAVING 等子句，分別計算各組件的 F1 分數，提供更細緻的錯誤分析。

Zhong et al.（2020）進一步提出**測試套件準確率**（Test Suite Accuracy），透過生成多組具有不同資料分布的資料庫實例來驗證 SQL 查詢的語義等價性。此方法可有效避免 EX 評估中因資料庫內容偶然一致而產生的假陽性問題，提供更可靠的語義正確性評估。

本研究同時採用 EM 與 EX 作為評估指標，以全面衡量模型在形式正確性與語義正確性兩個維度上的表現。由於本研究的目標資料庫 WP_M09 包含實際業務資料，EX 的評估結果具有較高的可信度。

### 2.5.2 Spider 基準資料集

Spider（Yu et al., 2018）是 Text-to-SQL 領域最具影響力的跨資料庫基準資料集，包含 10,181 個問題-SQL 配對，涵蓋 200 個資料庫與 138 個領域。Spider 按 SQL 查詢的複雜度分為四個難度等級：

- **Easy**：僅涉及單一 SELECT 與 WHERE 子句。
- **Medium**：包含 GROUP BY、ORDER BY 或多個 WHERE 條件。
- **Hard**：涉及子查詢、HAVING、巢狀結構等。
- **Extra Hard**：包含多層巢狀、集合運算（UNION/INTERSECT/EXCEPT）等。

Spider 的訓練集與測試集使用不同的資料庫，要求模型具備跨資料庫泛化能力。截至 2025 年，Spider 排行榜上的最佳系統已達到約 91% 的 EX 準確率，其中 DAIL-SQL（Gao et al., 2024）與 CHESS（Talaei et al., 2024）等基於 LLM 的方法表現最為突出。

值得注意的是，Spider 基準使用 SQLite 作為資料庫引擎，而本研究的目標環境為 Microsoft SQL Server 的 T-SQL 方言，兩者在語法上存在顯著差異（詳見 2.6.3 節），因此 Spider 上的評估結果無法直接類推至本研究場景。

### 2.5.3 BIRD 基準資料集

BIRD（Big Bench for Large-scale Database Grounded Text-to-SQL Evaluation）由 Li et al.（2024）提出，旨在解決 Spider 基準的若干限制。BIRD 的特色包括：

1. **大規模真實資料庫**：包含 95 個大型資料庫，總資料量達 33.4 GB，資料來源為 Kaggle 等真實資料平台。
2. **外部知識整合**：為每個問題提供外部知識提示（Knowledge Evidence），反映了真實場景中使用者需具備的領域知識。
3. **高難度查詢**：平均 SQL 長度較 Spider 更長，涉及更複雜的商業邏輯。
4. **執行效率評估**：除準確率外，還考量 SQL 的執行效率。

BIRD 的引入推動了 Text-to-SQL 研究從「形式正確」向「實際可用」的轉變，強調模型在真實業務場景中的應用能力。

### 2.5.4 其他相關資料集

| 資料集 | 年份 | 規模 | 特色 |
|--------|------|------|------|
| WikiSQL（Zhong et al., 2017） | 2017 | 80,654 | 單表查詢，規模最大 |
| SParC（Yu et al., 2019b） | 2019 | 4,298 | 多輪對話式 Text-to-SQL |
| CoSQL（Yu et al., 2019a） | 2019 | 3,007 | 對話狀態追蹤 + SQL |
| KaggleDBQA（Lee et al., 2021） | 2021 | 272 | 真實 Kaggle 資料庫 |
| DuSQL（Wang et al., 2020） | 2020 | 23,797 | 中文 Text-to-SQL |
| CSpider（Min et al., 2019） | 2019 | 10,181 | Spider 的中文翻譯版 |

---

## 2.6 中文自然語言處理與 Text-to-SQL

### 2.6.1 中文 NLP 的特殊挑戰

中文自然語言處理相較於英文面臨若干特殊挑戰：

1. **分詞問題**：中文缺乏天然的詞邊界標記（如英文的空格），需仰賴分詞模型進行斷詞。然而在 LLM 時代，基於子詞分詞器（Subword Tokenizer）如 BPE（Byte Pair Encoding）（Sennrich et al., 2016）的方法已逐漸取代傳統分詞工具。
2. **字元表意性**：中文字元本身即具有語義，同一字元在不同語境下可能有截然不同的意涵，增加了語義理解的複雜度。
3. **口語與書面語差異**：使用者以自然語言查詢資料庫時，常使用口語化的表達方式，與 SQL 所需的精確邏輯表述之間存在較大落差。

### 2.6.2 中文 Text-to-SQL 研究現況

中文 Text-to-SQL 研究起步較晚，但近年來已累積不少成果。CSpider（Min et al., 2019）將英文 Spider 資料集翻譯為中文，建立了首個跨資料庫的中文 Text-to-SQL 基準。DuSQL（Wang et al., 2020）則由百度建構，提供更大規模且更貼近中文表達習慣的資料集。

在多語言大型語言模型方面，Llama 3.1 在預訓練階段已納入大量中文語料，但在中文理解與生成能力上仍不及專為中文設計的模型。Qwen 系列（Bai et al., 2023）與 Yi 系列（01.AI, 2024）等以中文為重點的開源模型在中文 NLP 任務上展現了更優異的表現，值得作為未來替代基礎模型的選項進行評估。

### 2.6.3 繁體中文 Text-to-SQL 的應用場景

繁體中文 Text-to-SQL 的研究更為稀少，主要原因包括：（1）繁體中文訓練資料匱乏；（2）多數開源模型以簡體中文為主要支援語言；（3）台灣企業的資料庫系統多使用 Microsoft SQL Server，其 T-SQL 方言與學術研究中常用的 SQLite/MySQL 存在語法差異。

下表整理 T-SQL 與 SQLite/MySQL 在 Text-to-SQL 任務中常見的語法差異：

| 功能 | SQLite / MySQL | T-SQL (SQL Server) |
|------|---------------|-------------------|
| 限制筆數 | `LIMIT N` | `TOP N`（置於 SELECT 後） |
| Unicode 字串 | `'字串'` | `N'字串'` |
| 字串擷取 | `SUBSTR(col, start, len)` | `LEFT(col, len)` / `SUBSTRING(col, start, len)` |
| 字串連接 | `\|\|` 或 `CONCAT()` | `+` 或 `CONCAT()` |
| 日期函數 | `DATE()`, `strftime()` | `GETDATE()`, `DATEPART()`, `DATEDIFF()` |
| 布林值 | `TRUE` / `FALSE` | 無布林型態，以 `1` / `0` 或 `'Y'` / `'N'` 表示 |
| 分頁查詢 | `LIMIT N OFFSET M` | `OFFSET M ROWS FETCH NEXT N ROWS ONLY` |
| 型別轉換 | `CAST()` | `CAST()` / `CONVERT()` |

這些語法差異意味著以 SQLite 為基礎訓練的 Text-to-SQL 模型無法直接應用於 T-SQL 環境，需要透過特定領域的微調來適應目標 SQL 方言。

此外，繁簡中文的差異也對模型表現產生影響。Llama 3.1 的預訓練語料以簡體中文為主，繁體中文的覆蓋率相對不足。這導致繁體中文輸入可能被分詞器拆分為更多的子詞片段，降低 token 使用效率。透過微調，模型能逐步適應繁體中文的用詞習慣與表達方式，縮小繁簡差異帶來的性能落差。

本研究聚焦於繁體中文企業資料庫的 Text-to-SQL 任務，透過特定領域的資料集建構與業務規則嵌入，克服上述挑戰。此應用導向的研究方向，對於推動台灣企業智慧化資料查詢具有實務價值。

---

## 2.7 本章小結

本章回顧了與本研究密切相關的文獻與技術背景。Text-to-SQL 作為自然語言處理的核心任務之一，已從早期的規則式方法發展至基於大型語言模型的多階段管線方法，如 DIN-SQL、DAIL-SQL 和 CHESS 等。然而，現有研究主要以英文與 SQLite 環境為對象，在非英語語言與企業級 SQL 方言的支援上仍有不足。

Llama-3.1-8B-Instruct 作為目前最具代表性的開源大型語言模型之一，為 Text-to-SQL 微調提供了良好的基礎。DoRA 在 LoRA 基礎上引入權重方向-大小分解機制，配合 QLoRA 的 NF4 量化技術，使得在消費級 GPU 上以接近全參數微調品質進行大型語言模型微調成為可能。

在評估方面，精確匹配準確率與執行準確率各有其優缺點，本研究同時採用兩者以全面衡量模型表現。中文 Text-to-SQL、特別是繁體中文企業環境的應用研究仍處於起步階段。綜合文獻回顧，目前尚無針對繁體中文企業 T-SQL 環境，結合 DoRA 微調開源大型語言模型的 Text-to-SQL 研究，本研究期望填補此領域的研究空缺。

---

## 參考文獻

- 01.AI. (2024). Yi: Open foundation models by 01.AI. *arXiv preprint arXiv:2403.04652*.
- Androutsopoulos, I., Ritchie, G. D., & Thanisch, P. (1995). Natural language interfaces to databases–an introduction. *Natural Language Engineering*, 1(1), 29-81.
- Bai, J., Bai, S., Chu, Y., et al. (2023). Qwen technical report. *arXiv preprint arXiv:2309.16609*.
- Brown, T. B., Mann, B., Ryder, N., et al. (2020). Language models are few-shot learners. *Advances in Neural Information Processing Systems*, 33, 1877-1901.
- Chen, M., Tworek, J., Jun, H., et al. (2021). Evaluating large language models trained on code. *arXiv preprint arXiv:2107.03374*.
- Dettmers, T., Pagnoni, A., Holtzman, A., & Zettlemoyer, L. (2023). QLoRA: Efficient finetuning of quantized language models. *Advances in Neural Information Processing Systems*, 36.
- Gao, D., Wang, H., Li, Y., et al. (2024). Text-to-SQL empowered by large language models: A benchmark evaluation. *Proceedings of the VLDB Endowment*, 17(5), 1132-1145.
- Devlin, J., Chang, M. W., Lee, K., & Toutanova, K. (2019). BERT: Pre-training of deep bidirectional transformers for language understanding. *NAACL-HLT*, 4171-4186.
- Frantar, E., Ashkboos, S., Hoefler, T., & Alistarh, D. (2023). GPTQ: Accurate post-training quantization for generative pre-trained transformers. *ICLR*.
- Houlsby, N., Giurgiu, A., Jastrzebski, S., et al. (2019). Parameter-efficient transfer learning for NLP. *ICML*, 2790-2799.
- Hu, E. J., Shen, Y., Wallis, P., et al. (2022). LoRA: Low-rank adaptation of large language models. *ICLR*.
- Kirkpatrick, J., Pascanu, R., Rabinowitz, N., et al. (2017). Overcoming catastrophic forgetting in neural networks. *Proceedings of the National Academy of Sciences*, 114(13), 3521-3526.
- Lei, W., Wang, W., Ma, Z., et al. (2020). Re-examining the role of schema linking in text-to-SQL. *EMNLP*, 6943-6954.
- Lewis, M., Liu, Y., Goyal, N., et al. (2020). BART: Denoising sequence-to-sequence pre-training for natural language generation, translation, and comprehension. *ACL*, 7871-7880.
- Lan, Z., Chen, M., Goodman, S., et al. (2020). ALBERT: A lite BERT for self-supervised learning of language representations. *ICLR*.
- Lee, C. H., Polozov, O., & Richardson, M. (2021). KaggleDBQA: Realistic evaluation of text-to-SQL parsers. *ACL*, 2261-2273.
- Lester, B., Al-Rfou, R., & Constant, N. (2021). The power of scale for parameter-efficient prompt tuning. *EMNLP*, 3045-3059.
- Li, H., Zhang, J., Li, C., et al. (2023a). RESDSQL: Decoupling schema linking and skeleton parsing for text-to-SQL. *AAAI*, 13067-13075.
- Li, H., Zhang, J., Li, C., et al. (2024). Can LLM already serve as a database interface? A big bench for large-scale database grounded text-to-SQL. *NeurIPS*.
- Li, X. L., & Liang, P. (2021). Prefix-tuning: Optimizing continuous prompts for generation. *ACL*, 4582-4597.
- Lin, J., Zhao, Y., Fernandez, M., & Bertinetto, L. (2024). AWQ: Activation-aware weight quantization for LLM compression and acceleration. *MLSys*.
- Lin, X. V., Socher, R., & Xiong, C. (2020). Bridging textual and tabular data for cross-domain text-to-SQL semantic parsing. *EMNLP Findings*, 4870-4888.
- Liu, A., Hu, H., et al. (2024). DoRA: Weight-decomposed low-rank adaptation. *ICML*.
- Liu, J., et al. (2023). A comprehensive evaluation on ChatGPT for text-to-SQL. *arXiv preprint arXiv:2303.13547*.
- Liu, Y., Ott, M., Goyal, N., et al. (2019). RoBERTa: A robustly optimized BERT pretraining approach. *arXiv preprint arXiv:1907.11692*.
- Meta. (2024). The Llama 3 herd of models. *arXiv preprint arXiv:2407.21783*.
- Micikevicius, P., Narang, S., Alben, J., et al. (2018). Mixed precision training. *ICLR*.
- Min, Q., Shi, Y., & Zhang, Y. (2019). A pilot study for Chinese SQL semantic parsing. *EMNLP*, 3652-3658.
- Pourreza, M., & Rafiei, D. (2024). DIN-SQL: Decomposed in-context learning of text-to-SQL with self-correction. *NeurIPS*.
- Raffel, C., Shazeer, N., Roberts, A., et al. (2020). Exploring the limits of transfer learning with a unified text-to-text transformer. *Journal of Machine Learning Research*, 21(140), 1-67.
- OpenAI. (2023). GPT-4 technical report. *arXiv preprint arXiv:2303.08774*.
- Radford, A., Narasimhan, K., Salimans, T., & Sutskever, I. (2018). Improving language understanding by generative pre-training. *OpenAI*.
- Radford, A., Wu, J., Child, R., et al. (2019). Language models are unsupervised multitask learners. *OpenAI Blog*, 1(8), 9.
- Rajkumar, N., Li, R., & Baber, D. (2022). Evaluating the text-to-SQL capabilities of large language models. *arXiv preprint arXiv:2204.00498*.
- Scholak, T., Schucher, N., & Bahdanau, D. (2021). PICARD: Parsing incrementally for constrained auto-regressive decoding from language models. *EMNLP*, 9895-9901.
- Sennrich, R., Haddow, B., & Birch, A. (2016). Neural machine translation of rare words with subword units. *ACL*, 1715-1725.
- Touvron, H., Lavril, T., Izcard, G., et al. (2023a). LLaMA: Open and efficient foundation language models. *arXiv preprint arXiv:2302.13971*.
- Touvron, H., Martin, L., Stone, K., et al. (2023b). Llama 2: Open foundation and fine-tuned chat models. *arXiv preprint arXiv:2307.09288*.
- Talaei, S., Pourreza, M., Chang, Y., et al. (2024). CHESS: Contextual harnessing for efficient SQL synthesis. *arXiv preprint arXiv:2405.16755*.
- Vaswani, A., Shazeer, N., Parmar, N., et al. (2017). Attention is all you need. *Advances in Neural Information Processing Systems*, 30.
- Wang, L., Zhang, A., Wu, K., et al. (2020). DuSQL: A large-scale and pragmatic Chinese text-to-SQL dataset. *EMNLP*, 6923-6935.
- Warren, D. H. D., & Pereira, F. C. N. (1982). An efficient easily adaptable system for interpreting natural language queries. *American Journal of Computational Linguistics*, 8(3-4), 110-122.
- Woods, W. A. (1973). Progress in natural language understanding: An application to lunar geology. *AFIPS Conference Proceedings*, 42, 441-450.
- Yu, T., Zhang, R., Yang, K., et al. (2018). Spider: A large-scale human-labeled dataset for complex and cross-domain semantic parsing and text-to-SQL task. *EMNLP*, 3911-3921.
- Yu, T., Zhang, R., Er, H. Y., et al. (2019a). CoSQL: A conversational text-to-SQL challenge towards cross-domain natural language interfaces to databases. *EMNLP*, 1962-1979.
- Yu, T., Zhang, R., Yasunaga, M., et al. (2019b). SParC: Cross-domain semantic parsing in context. *ACL*, 4511-4523.
- Zhong, R., Yu, T., & Klein, D. (2020). Semantic evaluation for text-to-SQL with distilled test suites. *EMNLP*, 396-411.
- Zhong, V., Xiong, C., & Socher, R. (2017). Seq2SQL: Generating structured queries from natural language using reinforcement learning. *arXiv preprint arXiv:1709.00103*.
