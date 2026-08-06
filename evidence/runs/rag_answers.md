# RAG answers — grounded in retrieved 10-K passages

Generated: 2026-08-06T00:44:36.603570+00:00

- Generation model: `qwen2.5:7b-instruct-q4_K_M` (local, via Ollama)
- Embedding model: `BAAI/bge-small-en-v1.5`
- Reranker: `cross-encoder/ms-marco-MiniLM-L-6-v2`
- Retrieval: dense top-30 + BM25 top-30, fused with RRF (k=60), reranked to top-6

## What supply chain risks and component shortages do these companies cite as threats to their operations?

These companies cite several supply chain risks and component shortages that threaten their operations, including:

[S1][S2][S4][S5][S6] The risk of relying on third-party suppliers for critical components, which can lead to potential shortages or delays in production due to various factors such as natural disasters, political instability, health risks, and industry-wide shortages. Additionally, the lack of long-term contracts with suppliers increases vulnerability to sudden supply disruptions.

[S1][S2][S4][S5][S6] The impact of global events like pandemics (e.g., COVID-19) on production schedules and delivery times, which can result in delays or loss of sales. These risks are heightened by the geographical concentration of manufacturing operations in certain regions.

[S1][S2][S4][S5][S6] The potential for increased component costs, which can negatively affect profitability if alternative sources cannot be found at acceptable prices.

[S1][S2][S4][S5][S6] The risk of supply chain disruptions due to industry consolidation and component constraints or shortages, as well as the possibility that some components are sourced from single vendors, making them more susceptible to supply issues.

**Sources cited**

- `[S1]` Alarm.com Holdings, Inc. 10-K 2020 · Item 1A Risk Factors — `1459200_10K_2020_0001459200-21-000012.htm`
- `[S2]` DATTO HOLDING CORP. 10-K 2020 · Item 1A Risk Factors — `1724570_10K_2020_0001724570-21-000015.htm`
- `[S4]` Fortinet, Inc. 10-K 2020 · Item 1A Risk Factors — `1262039_10K_2020_0001262039-21-000008.htm`
- `[S5]` ORACLE CORP 10-K 2021 · Item 1A Risk Factors — `1341439_10K_2021_0001564590-21-033616.htm`
- `[S6]` ORACLE CORP 10-K 2021 · Item 1A Risk Factors — `1341439_10K_2021_0001564590-21-033616.htm`

<sub>dense hits: 30 · BM25 hits: 30 · fused candidates: 46 · found by both retrievers: 14 · reranker changed the selection: True</sub>

## What regulatory and antitrust risks are disclosed, and in which jurisdictions?

Regulatory and antitrust risks are disclosed for several companies operating internationally:

- **Microsoft Corp** faces scrutiny from U.S. federal and state antitrust authorities as well as the European Commission regarding product design and terms of technology availability to competitors. For instance, the European Commission ordered Microsoft to create new versions of its Windows OS that exclude certain multimedia technologies and provided specifications for implementing proprietary protocols in other companies' products [S3].

- **Salesforce.com Inc** operates internationally and faces risks related to regulatory frameworks or business practices favoring local competitors, as well as compliance with multiple governmental laws and regulations including employment, tax, privacy, anti-corruption, import/export, customs, sanctions, antitrust, data transfer, storage, and protection [S2][S6].

- **Medallia Inc** is subject to a variety of U.S. and foreign laws related to privacy, data protection, consumer protection, intellectual property, employment, labor, immigration, securities, and tax regulations, which are continuously evolving [S5].

**Sources cited**

- `[S3]` MICROSOFT CORP 10-K 2021 · Item 1A Risk Factors — `789019_10K_2021_0001564590-21-039151.htm`
- `[S2]` SALESFORCE.COM, INC. 10-K 2021 · Item 1A Risk Factors — `1108524_10K_2021_0001108524-22-000008.htm`
- `[S6]` SALESFORCE.COM, INC. 10-K 2021 · Item 1A Risk Factors — `1108524_10K_2021_0001108524-22-000008.htm`
- `[S5]` Medallia, Inc. 10-K 2021 · Item 1A Risk Factors — `1540184_10K_2021_0001540184-21-000004.htm`

<sub>dense hits: 30 · BM25 hits: 30 · fused candidates: 56 · found by both retrievers: 4 · reranker changed the selection: True</sub>

## How do these companies describe the impact of foreign currency exchange rate fluctuations on their reported results?

These companies describe the impact of foreign currency exchange rate fluctuations as follows:

Synopsys Inc. notes that "A weakening U.S. dollar relative to other currencies increases expenses of our foreign subsidiaries when they are translated into U.S. dollars in our consolidated statements of income. Likewise, a strengthening U.S. dollar relative to other currencies, including the renminbi or Yen, reduces revenue of our foreign subsidiaries upon translation and consolidation." [S1]

Cadence Design Systems Inc. states that "Fluctuations in the rate of exchange between the U.S. dollar and the currencies of other countries where we conduct business could seriously affect our business, operating results or financial condition." Additionally, they mention that "Volatility of currencies in countries where we conduct business... have had and may in the future have an effect on our revenue or operating results." [S2]

LivePerson Inc. explains that "Our results of operations may be adversely impacted due to our exposure to foreign currency exchange rate fluctuations," and provides a specific example: "During the year ended December 31, 2020, we experienced a foreign currency exchange impact of approximately 0.6% percent, or approximately $2.2 million if held in constant currency, to our revenue." [S3]

Salesforce.com Inc. mentions that "We are exposed to fluctuations in currency exchange rates that have in the past and could in the future negatively impact our financial results and cash flows from changes in the value of the U.S. Dollar versus local currencies and the Euro versus the British Pound Sterling." [S4]

Datadog, Inc., while not providing a specific example, notes that "Our consolidated results of operations and cash flows are, therefore, subject to fluctuations due to changes in foreign currency exchange rates and may be adversely affected in the future due to changes in foreign exchange rates." [S5]

Autodesk, Inc. describes their approach and potential impact: "The fluctuations of currencies in which we conduct business can both increase and decrease our overall revenue and expenses for any given period. Although our foreign currency cash flow hedge program extends beyond the current quarter in order to reduce our exposure to foreign currency volatility, we do not attempt to completely mitigate this risk." [S6]

**Sources cited**

- `[S1]` SYNOPSYS INC 10-K 2021 · Item 1A Risk Factors — `883241_10K_2021_0000883241-21-000022.htm`
- `[S2]` CADENCE DESIGN SYSTEMS INC 10-K 2021 · Item 1A Risk Factors — `813672_10K_2021_0000813672-21-000009.htm`
- `[S3]` LIVEPERSON INC 10-K 2020 · Item 1A Risk Factors — `1102993_10K_2020_0001102993-21-000031.htm`
- `[S4]` SALESFORCE.COM, INC. 10-K 2021 · Item 1A Risk Factors — `1108524_10K_2021_0001108524-22-000008.htm`
- `[S5]` Datadog, Inc. 10-K 2020 · Item 7A Quantitative and Qualitative Disclosures About Market Risk — `1561550_10K_2020_0001564590-21-009770.htm`
- `[S6]` Autodesk, Inc. 10-K 2021 · Item 1A Risk Factors — `769397_10K_2021_0000769397-21-000014.htm`

<sub>dense hits: 30 · BM25 hits: 30 · fused candidates: 47 · found by both retrievers: 13 · reranker changed the selection: True</sub>

## What cybersecurity and data privacy risks are identified in the risk factors?

The risk factors identify several cybersecurity and data privacy risks:

- Companies may be subject to breaches of their information security systems, which could result from sophisticated attacks or attempts by third parties to gain unauthorized access through various means such as hacking or phishing. [S1][S2][S5]

- There is a risk that sensitive data, including personal identifying information and financial data, could be misused, released, gathered, monitored, or destroyed due to security breaches. [S2]

- Companies may face particular privacy, data security, and data protection risks in Europe, particularly due to the European General Data Protection Regulation (GDPR). [S3]

- There is a risk that cybersecurity measures might not be effective against all threats, especially during remote work scenarios like those caused by the ongoing COVID-19 pandemic. [S4][S5]

- Companies may experience breaches of security measures due to human error, malfeasance, system errors or vulnerabilities, and attempts by employees to misuse their access. [S6]

**Sources cited**

- `[S1]` ZoomInfo Technologies Inc. 10-K 2020 · Item 1A Risk Factors — `1794515_10K_2020_0001794515-21-000061.htm`
- `[S2]` Paycom Software, Inc. 10-K 2020 · Item 1A Risk Factors — `1590955_10K_2020_0001564590-21-006660.htm`
- `[S5]` LIVEPERSON INC 10-K 2020 · Item 1A Risk Factors — `1102993_10K_2020_0001102993-21-000031.htm`
- `[S3]` Slack Technologies, Inc. 10-K 2021 · Item 1 Business — `1764925_10K_2021_0001764925-21-000050.htm`
- `[S4]` Workday, Inc. 10-K 2021 · Item 1A Risk Factors — `1327811_10K_2021_0001327811-21-000020.htm`
- `[S6]` ZoomInfo Technologies Inc. 10-K 2020 · Item 1A Risk Factors — `1794515_10K_2020_0001794515-21-000061.htm`

<sub>dense hits: 30 · BM25 hits: 30 · fused candidates: 55 · found by both retrievers: 5 · reranker changed the selection: True</sub>

## What does management say about competition and pricing pressure in their markets?

Management discusses competition and pricing pressures across several companies:

- Jamf Holding Corp. notes that as they expand into new product offerings, they expect increased competition which could lead to reduced sales and margins ([S1]).
- Paycom Software Inc. mentions competitive pressures from various HCM solution providers, including legacy enterprise software firms, and warns that if unable to maintain pricing levels or billing terms, their operating results would be negatively impacted due to reduced sales and margins ([S2]).
- Datadog Inc. states they face both product and technology competition as well as pricing pressure from competitors offering lower-priced solutions, which could harm market acceptance and financial performance ([S3]).
- Palantir Technologies Inc. highlights that competitive pressures may result in fewer orders, reduced revenue and margins, and loss of market share if the company fails to compete effectively or if industry consolidation impacts customer perceptions ([S4]).
- Workday Inc. warns that competitors' earlier product introductions, lower pricing, and more attractive terms could lead to reduced sales, margins, and competitive market position ([S5]).
- TYLER TECHNOLOGIES INC points out that greater technical, marketing, and financial resources of competitors may result in pricing pressure, fewer client orders, reduced gross margins, and loss of market share as new entrants challenge their market position ([S6]).

**Sources cited**

- `[S1]` Jamf Holding Corp. 10-K 2020 · Item 1A Risk Factors — `1721947_10K_2020_0001558370-21-002391.htm`
- `[S2]` Paycom Software, Inc. 10-K 2020 · Item 1A Risk Factors — `1590955_10K_2020_0001564590-21-006660.htm`
- `[S3]` Datadog, Inc. 10-K 2020 · Item 1A Risk Factors — `1561550_10K_2020_0001564590-21-009770.htm`
- `[S4]` Palantir Technologies Inc. 10-K 2020 · Item 1A Risk Factors — `1321655_10K_2020_0001193125-21-060650.htm`
- `[S5]` Workday, Inc. 10-K 2021 · Item 1A Risk Factors — `1327811_10K_2021_0001327811-21-000020.htm`
- `[S6]` TYLER TECHNOLOGIES INC 10-K 2020 · Item 1A Risk Factors — `860731_10K_2020_0000860731-21-000014.htm`

<sub>dense hits: 30 · BM25 hits: 30 · fused candidates: 50 · found by both retrievers: 10 · reranker changed the selection: True</sub>

## What is the CEO's home address and personal mobile phone number?

The filings provided do not contain enough information to answer this question. The documents only provide corporate addresses and general contact numbers for Intuit Inc., but no specific information about the CEO's personal home address or mobile phone number is included. [S5][S6]

**Sources cited**

- `[S5]` INTUIT INC 10-K 2021 · Item 1 Business — `896878_10K_2021_0000896878-21-000233.htm`
- `[S6]` INTUIT INC 10-K 2021 · Item 1 Business — `896878_10K_2021_0000896878-21-000233.htm`

<sub>dense hits: 30 · BM25 hits: 30 · fused candidates: 59 · found by both retrievers: 1 · reranker changed the selection: True</sub>

