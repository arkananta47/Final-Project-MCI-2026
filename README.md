# 🚀 OPREC MCI 2026 Final Project — DustiniaDelixia_Groceria - Operational Analysis  

> Tugas Final Project Lab MCI 2026 · End-to-End Modern Data Stack Implementation

---

# 👥 Data Diri Author

| Nama | NRP |
|---|---|
| Muh. Aqil Alqadri Syahid | 5025241161 |

---


# 🧰 Technology Stack

**Pipeline Orchestration & Data Visualization**

```text
Apache Airflow → PySpark → ClickHouse → Metabase / Power BI
```

---

# 📋 Daftar Isi
1. **🎯 Overview**  
2. **📦 Dataset Brief**  
3. **🎯 Business Problem** 
4. **😊 Sentiment Analysis**
5. **🏗 Arsitektur Pipeline**
6. **📦 Penjelasan Script**
7. **🗄 ClickHouse Schema**
8. **📊 Metabase Visualization**

---

# 🎯 Overview

Project ini membangun **end-to-end data pipeline untuk menganalisis performa operasional pengiriman pada DustiniaDelixia_Groceria (dataset berasal dari Olist E-Commerce)** menggunakan Apache Airflow, PySpark, ClickHouse, dan Metabase.

Tujuan utama proyek adalah mengevaluasi efektivitas proses fulfillment dan pengiriman pesanan berdasarkan data transaksi, data pengiriman, serta review pelanggan.

Pipeline melakukan:

* **Membaca dataset Parquet dari Data Lake**
* **Menggabungkan data order, customer, seller, item, dan review**
* **Menghitung KPI operasional pengiriman**
* **Melakukan sentiment analysis sederhana terhadap review pelanggan**
* **Memuat hasil analisis ke ClickHouse**
* **Menyajikan insight melalui dashboard Metabase**

---

# 📦 Dataset Brief
Dataset yang digunakan adalah Olist Brazilian E-Commerce Dataset. Terdapat 11 dataset yang bisa digunakan, namun karena saya memilih persona 3 alias **Operational Analyst**, maka saya telah menyeleksi dataset-dataset yang menurut saya relevan untuk digunakan dalam proses analisis nantinya.

| Dataset       | Keterangan                        |
| ------------- | --------------------------------- |
| orders        | Informasi pesanan                 |
| order_items   | Informasi item dan shipping limit |
| order_reviews | Review pelanggan                  |
| customers     | Informasi customer                |
| sellers       | Informasi seller                  |

---

# 🎯 Business Problem

Analisis dilakukan untuk menjawab beberapa pertanyaan bisnis:

* **Berapa persen pesanan terlambat?**
* **State mana yang memiliki keterlambatan tertinggi?**
* **Seller mana yang paling sering melanggar SLA?**
* **Apakah keterlambatan memengaruhi kepuasan pelanggan?**
* **Apakah performa pengiriman membaik dari waktu ke waktu?**

*Keywords:* ```"Bagaimana performa pengiriman Olist, siapa yang menyebabkan masalah, wilayah mana yang bermasalah, apakah keterlambatan memengaruhi kepuasan pelanggan, dan apa keluhan utama pelanggan."```

---

# 😊 Sentiment Analysis
Selain menganalisis performa operasional pengiriman, pipeline ini juga melakukan ***analisis sentimen terhadap ulasan pelanggan*** untuk memahami bagaimana pengalaman pengiriman memengaruhi tingkat kepuasan konsumen.

_Proses analisis_ dilakukan dengan mengekstrak komentar pelanggan pada dataset review, kemudian mengidentifikasi kata-kata yang sering diasosiasikan dengan pengalaman pengiriman yang **positif, negatif, maupun netral**.

#### 🎯 Tujuan Analisis
Analisis ini bertujuan untuk:
* Mengukur persepsi pelanggan terhadap layanan pengiriman.
* Mengidentifikasi faktor utama yang menyebabkan kepuasan atau ketidakpuasan pelanggan.
* Menemukan hubungan antara keterlambatan pengiriman dengan sentimen review.
* Membantu perusahaan memahami area operasional yang perlu ditingkatkan.

#### 🔍 Metode yang Digunakan
Pipeline menerapkan pendekatan **Keyword-Based Sentiment Classification** pada review berbahasa Portugis.

Setiap komentar pelanggan akan dianalisis untuk mencari kata kunci yang berkaitan dengan pengalaman pengiriman.

***Positive Delivery Experience***
Contoh kata kunci yang menunjukkan pengalaman positif:
```
rápido
antes
prazo
adiantado
parabéns
excelente
bem-embalado
certinho
recomendo
```
*Makna bisnis:*
- Pengiriman lebih cepat dari estimasi.
- Barang diterima tepat waktu.
- Kondisi barang baik saat diterima.
- Pelanggan bersedia merekomendasikan layanan.


***Negative Delivery Experience***
Contoh kata kunci yang menunjukkan pengalaman negatif:
```
atraso
atrasado
demora
demorou
amassado
quebrado
danificado
eternidade
rastreamento
correios
horrível
péssimo
extraviado
sumiu
reclamação
```
*Makna bisnis:*
- Pengiriman terlambat.
- Barang rusak selama proses distribusi.
- Masalah pelacakan pengiriman.
- Paket hilang atau tidak diterima pelanggan.
- Keluhan terhadap kualitas layanan logistik.

---

# 🏗 Arsitektur Pipeline
```text
                ┌────────────────────┐
                │   Olist Dataset    │
                │     Parquet        │
                └─────────┬──────────┘
                          │
                          ▼
               process_datasets_spark.py

                          │

      ┌───────────────────┼───────────────────┐
      │                   │                   │

      ▼                   ▼                   ▼
fact_operational    KPI & Summary    Sentiment Analysis
    Orders              Tables

      │                   │                   │

      └───────────────────┬───────────────────┘
                          ▼

                 ClickHouse Warehouse

                          │

                 Metabase Dashboard
```

---

# 📦 Penjelasan Script

# 1. fetch_datasets.py
Script untuk proses **Extract** dan penyimpanan ke Data Lake.

Fungsi:
- Mengambil dataset Olist dari sumber data
- Membaca seluruh file CSV yang diperlukan
- Mengonversi dataset ke format Parquet
- Menyimpan hasil ke folder ```data_lake```

## Output

```text
customers.parquet
orders.parquet
order_items.parquet
order_reviews.parquet
sellers.parquet
```

# 2. process_datasets_spark.py
Script utama untuk proses **Transform dan Load** menggunakan PySpark.

Script ini:
- Membaca dataset Parquet dari Data Lake
- Melakukan join antar tabel Olist
- Menghitung KPI operasional pengiriman
- Melakukan sentiment analysis sederhana menggunakan keyword matching pada review pelanggan

#### Beberapa database yang dibuat
```
fact_operational_orders
kpi_summary
state_summary
seller_summary
monthly_trend
fact_review_sentiment
```

# 3. datasets_pipeline.py
Script Apache Airflow DAG yang mengatur seluruh workflow ETL.

Workflow:
```text
     start
       ↓
  fetch_datasets
       ↓
process_datasets_spark
       ↓
      end
```

---

# 🗄 ClickHouse Schema

Database:

```sql
fpmci2026_db
```

## Tables

### 1. fact_operational_orders
Fact table utama.
```
order_id
customer_id
seller_id
customer_state
seller_state
review_score
delivery_delay_days
processing_time_days
shipping_time_days
is_late
order_month
```

### 2. kpi_summary
Ringkasan KPI global.
```
total_orders
avg_delay_days
avg_processing_days
avg_shipping_days
avg_review_score
late_rate
```

### 3. state_summary
Ringkasan performa per state.
```
customer_state
total_orders
avg_delay_days
late_rate
avg_review_score
```

### 4. seller_summary
Ringkasan performa seller.
```
seller_id
total_orders
sla_breach_rate
avg_delay_days
avg_review_score
```

### 5. monthly_trend
Trend bulanan.
```
order_month
total_orders
avg_delay_days
late_rate
```

### 6. fact_review_sentiment
Hasil sentiment analysis.
```
order_id
review_score
sentiment_label
keyword
delivery_delay_days
customer_state
seller_id
```

---

# 📊 Metabase Visualization
### 📈 Dashboard Queries & Insights

#### Q1 — Executive KPI Overview

Visualisasi:

![alt text](assets/q1.png)

## Insight
Visualisasi ini menampilkan ringkasan performa operasional pengiriman secara keseluruhan.

Insight yang dapat diperoleh:
* Mengetahui total jumlah pesanan yang berhasil diproses dalam periode pengamatan.
* Mengukur rata-rata keterlambatan pengiriman dibanding estimasi yang diberikan kepada pelanggan.
* Mengetahui persentase pesanan yang terlambat (late delivery rate).
* Mengukur tingkat kepuasan pelanggan melalui rata-rata review score.
* Menjadi indikator utama untuk mengevaluasi efektivitas sistem logistik secara keseluruhan.

---

## Q2: On-Time vs Late Delivery Rate

Visualisasi:

![alt text](assets/q2.png)

## Insight
Visualisasi ini membandingkan jumlah pesanan yang dikirim tepat waktu dengan pesanan yang terlambat.

Insight:
* Mengetahui proporsi keberhasilan pengiriman tepat waktu.
* Mengukur tingkat kepatuhan terhadap target pengiriman.
* Mengidentifikasi seberapa besar masalah keterlambatan dalam sistem logistik.
* Menjadi indikator utama kualitas layanan pengiriman kepada pelanggan.
* Dapat digunakan untuk mengevaluasi efektivitas proses fulfillment dan distribusi.

---

## Q3 — Top 10 States dengan Keterlambatan Terbesar

Visualisasi:

![alt text](assets/q3.png)

## Insight
Visualisasi ini menampilkan wilayah pelanggan dengan rata-rata keterlambatan pengiriman tertinggi.

Insight:
* Mengidentifikasi state yang memiliki performa pengiriman paling buruk.
* Mengetahui daerah yang berpotensi mengalami kendala distribusi atau infrastruktur logistik.
* Membantu perusahaan menentukan prioritas perbaikan operasional berdasarkan wilayah.
* Dapat digunakan untuk evaluasi lokasi warehouse atau distribusi regional.
* Memberikan gambaran persebaran masalah keterlambatan secara geografis.

---

## Q4 — State dengan Kepuasan Pelanggan Terendah

Visualisasi:

![alt text](assets/q4.png)

## Insight
Visualisasi ini menunjukkan wilayah dengan rata-rata review score terendah.

Insight:
* Mengidentifikasi daerah yang memiliki tingkat kepuasan pelanggan paling rendah.
* Membantu mencari hubungan antara keterlambatan pengiriman dan kepuasan pelanggan.
* Menentukan wilayah yang membutuhkan peningkatan kualitas layanan.
* Menjadi indikator kualitas pengalaman pelanggan berdasarkan lokasi geografis.
* Dapat digunakan untuk evaluasi performa operasional per wilayah.

---

## Q5 — Top Seller dengan SLA Breach Tertinggi

Visualisasi:

![alt text](assets/q5.png)

## Insight
Visualisasi ini menampilkan seller dengan tingkat pelanggaran SLA pengiriman tertinggi.

Insight:
* Mengidentifikasi seller yang paling sering melewati batas waktu pengiriman.
* Mengetahui seller yang berpotensi menyebabkan pengalaman pelanggan yang buruk.
* Membantu tim operasional melakukan monitoring terhadap seller bermasalah.
* Menjadi dasar evaluasi kepatuhan seller terhadap standar layanan perusahaan.
* Dapat digunakan untuk program pembinaan atau penalti terhadap seller tertentu.

---

## Q6 — Top Seller dengan Performa Terbaik

Visualisasi:

![alt text](assets/q6.png)

## Insight
Visualisasi ini menunjukkan seller dengan review score tertinggi dan performa pengiriman yang baik.

Insight:
* Mengidentifikasi seller dengan tingkat kepuasan pelanggan terbaik.
* Mengetahui seller yang konsisten memberikan layanan berkualitas.
* Dapat digunakan sebagai benchmark bagi seller lainnya.
* Membantu perusahaan dalam program penghargaan atau promosi seller terbaik.
* Menunjukkan hubungan antara kualitas layanan dan kepuasan pelanggan.

---

## Q7 — Monthly Delivery Trend

Visualisasi:

![alt text](assets/q7.png)

## Insight
Visualisasi ini memperlihatkan tren performa pengiriman dari bulan ke bulan.

Insight:
* Mengetahui apakah tingkat keterlambatan membaik atau memburuk dari waktu ke waktu.
* Mengidentifikasi periode dengan lonjakan keterlambatan pengiriman.
* Membantu mendeteksi dampak musim tertentu terhadap performa logistik.
* Menjadi dasar evaluasi strategi operasional jangka panjang.
* Dapat digunakan untuk memprediksi kebutuhan kapasitas logistik di masa depan.

---

## Q8 — Sentiment Distribution

Visualisasi:

![alt text](assets/q8.png)

## Insight
Visualisasi ini menampilkan distribusi sentimen pelanggan berdasarkan komentar review.

Insight:
* Mengukur proporsi pelanggan yang memberikan pengalaman positif, netral, atau negatif.
* Mengetahui persepsi pelanggan terhadap layanan pengiriman secara keseluruhan.
* Membantu mengevaluasi kualitas layanan dari sudut pandang pelanggan.
* Menjadi indikator non-finansial untuk mengukur customer experience.
* Dapat digunakan untuk memantau perubahan sentimen pelanggan dari waktu ke waktu.

---

## Q9 — Sentiment vs Delivery Delay

Visualisasi:

![alt text](assets/q9.png)

## Insight
Visualisasi ini menganalisis hubungan antara sentimen pelanggan dan tingkat keterlambatan pengiriman.

Insight:
* Mengetahui apakah keterlambatan pengiriman berpengaruh terhadap sentimen pelanggan.
* Mengidentifikasi rata-rata keterlambatan pada review positif, netral, dan negatif.
* Membuktikan dampak operasional logistik terhadap pengalaman pelanggan.
* Membantu perusahaan memahami faktor utama yang memicu keluhan pelanggan.
* Menjadi dasar pengambilan keputusan untuk meningkatkan kepuasan pelanggan melalui perbaikan proses pengiriman.

---

## Q10 — Review Score Distribution

Visualisasi:

![alt text](assets/q10.png)

## Insight
Visualisasi ini menunjukkan distribusi rating yang diberikan pelanggan setelah pesanan diterima.

Insight:
* Mengetahui pola kepuasan pelanggan berdasarkan skor review 1–5.
* Mengidentifikasi apakah mayoritas pelanggan merasa puas terhadap layanan yang diberikan.
* Membantu mendeteksi adanya ketidakpuasan pelanggan secara massal.
* Menjadi indikator kualitas layanan secara langsung dari perspektif pelanggan.
* Dapat digunakan untuk mengukur keberhasilan strategi peningkatan kualitas operasional dan pengiriman.

---

# ✅ Hasil Pipeline

Pipeline berhasil:
- Membaca data Olist dari Data Lake (Parquet).
- Melakukan transformasi dan analisis menggunakan PySpark.
- Menghitung KPI operasional pengiriman dan SLA.
- Melakukan sentiment analysis pada customer review.
- Memuat hasil analisis ke ClickHouse.
- Menyediakan dashboard interaktif melalui Metabase.
- Menghasilkan insight terkait performa pengiriman, seller reliability, dan customer satisfaction.
