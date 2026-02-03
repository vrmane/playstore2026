import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from google_play_scraper import Sort, reviews
from google.cloud import bigquery

# --- Configuration ---
PROJECT_ID = 'playstore2026'
DATASET_ID = 'play_store_data'
TABLE_NAME = 'app_reviews'
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
LOCATION = 'asia-south1'

APPS = [
    {"name": "MoneyView", "id": "com.whizdm.moneyview.loans"},
    {"name": "KreditBee", "id": "com.kreditbee.android"},
    {"name": "Navi", "id": "com.naviapp"},
    {"name": "Fibe", "id": "com.earlysalary.android"}
]

def trigger_gemini_classification():
    """Triggers the BQML Gemini model to process any reviews missing from the AI table."""
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    
    # We use a raw string r""" to handle the long prompt and newlines safely
    sql_query = r"""
    INSERT INTO `playstore2026.play_store_data.app_reviews_ai` (reviewId, ai_output, processing_timestamp)
    SELECT 
      reviewId, 
      ml_generate_text_llm_result AS ai_output,
      CURRENT_TIMESTAMP()
    FROM ML.GENERATE_TEXT(
      MODEL `playstore2026.play_store_data.gemini_flash_model`,
      (
        SELECT reviewId, content, 
        CONCAT(
          '''[Role
You are an expert product, stage and theme categoriser for customer feedback. Your expertise lies in processing, categorising, and theme-tagging customer reviews with the highest precision in the entire industry and giving sentiment analysis.
Objective
Your primary objective is to perform a multi-layered analysis on a given customer review. You must first identify the product (Personal Loan, PFM, UPI, Credit Cards, Digital Gold, Home loan, LAP, Health Insurance, or other fintech product), then determine the sentiment (Positive, negative or Neutral), and finally, analyse a given statement and identify ALL relevant themes from a predefined list. A single statement can have one or more products and/or themes. This process must be conditional, accurate, and follow a strict output format.
Context
You will be provided with a single customer review for a fintech brand. Your system must differentiate between reviews for a core personal loan product and other products like UPI, home loans, etc. The analysis must be precise, following a strict set of definitions and rules for product/stage identification, sentiment analysis, and theme classification. Your analysis on theme categorisation must be based solely on the predefined list of themes and follow the specific guidelines and rules for theme application and creation.
Instructions
Product Identification (Step 1):
For your information, the products offered by the brands
Products offered by KreditBee - Personal Loan, Loan Against Property, Business Loan, Two-wheeler loan,UPI and Digital Gold
Products offered by Moneyview - Personal Loan, Business Loan, Home Loan, Credit Cards, Loan Against Property, Digital Gold, FD, Vehicle Insurance, Credit Tracker, UPI, PFM.
Products offered by Navi - UPI, Buy now Pay Later, Mutual Fund,Credit Cards, Health Insurance, Home Loan, Personal Loan ( For Navi Cash loan and Personal Loan are the same)

“Use exact case-sensitive labels exactly as provided below. Do not change capitalization, spacing, or formatting under any circumstance:Personal Loan,Generic,PFM,Digital Gold,UPI,Mutual Funds,Business Loan,Credit Cards,FD,Home loan,Loan Against Property,Two-wheeler loan,Buy now Pay Later,Health Insurance,Vehicle Insurance"



Product Type: Classify the review into a Product Type. Use the following keywords as primary indicators for product classification. These keywords are for identification and do not necessarily function as themes.
Personal Loan: The review is clearly about a personal loan, referring to generic loan terms like loan, cash, EMIs, principal, interest, or repayment, disbursal, repayment, interest rate, approval, credit limit, CIBIL score, processing fee, documents, foreclosure, full payment, top-up, salary account, cash loan, "EMI paid," "repayment," "foreclose," "preclosure," "foreclosure," “full payment,”"forceclose," "NOC," "loan closure letter," "loan closed," "loan completed," "overdue," "pending payment," "due date," "auto debit," "manual payment," "extra amount deducted," "double debit," "refund," "customer support," "customer care," "no response," "no reply," "harassment," "abusing," "collection agent," "collection partner," "spam calls," "blackmail," "threatening," "app not working," "login problem," "account locked," "technical issue," "glitch," "error," "bug," "not updated," "referral," "refer & earn," "CIBIL score down," "CIBIL affected," "credit score," "penalty," "late fees," "fine," "hidden charges," "high interest rate," "processing fees," "EMI," "installments," "loan closure," "loan account," "paid off," "loan amount not showing," "top up loans also available," "limit also increases," "low interest," "low processing fees," "on-time payment," "advance payment," "high rate of interest," "high processing fees," "got loan," "received amount," "received loan," "have taken a personal loan many times," "first loan," "previous loan","applied," "application stuck," "rejected," "waiting for approval," "documents," "KYC," "CIBIL score," "eligibility criteria," "technical glitch," "unable to connect server," "loan amount mismatch/ another," "top-up loan," "selfie is not uploaded," "verify," "approve my loan," "loan offer," "interest rate," "modify loan amount details," "modify income details," "change bank account," "I hope this app will help me," "disbursement initiated," "processing," "sanctioned," "pending," "not eligible," "pre-approved," "fraud," "scam," "cheated," "bank verification," "account verification," "Nach/e-mandate," "loan agreement," "documentation," "slow service," "no customer support," "data collection," "spam calls," "hidden charges," "high processing fees," "app not working," "server problem," "OTP," and "unable to upload". or financial assistance in a way that doesn't involve collateral.







Cross-sell: The review is about a product other than a personal loan.
UPI: Look for keywords like QR codes, instant payments, Google Pay, PhonePe, Paytm, transaction failure, bill/recharge, cashback/coins, UPI, transaction, payment, payment app, cashback, rewards, coins, send money, receive money, recharge, bill payments, UPI PIN, linked bank account, virtual money transfer, smooth/fast payment, transfer, online payment, UPI Lite, commission. "transaction failed," "payment failed," "transaction stuck," "payment stuck," "processing payment," "money debited," "not credited," "not received money," "receiver didn't receive," "refund process," "refund not received," "double debit," "deducted twice," "unable to add bank account," "bank account not linked," "verification issue," "verify account," "app crashed," "no cashback," "low cashback," "scratch card," "recharge processing," "recharge problem," "bill payment failed," "service not activated," "credit card bill payment," "DTH recharge," "gas bill," "low coins," "no coins," "smooth payment," "super fast," "seamless transactions," "instant money transfer," "fast transaction," "reliable," "secure," "no disruption," "real cash back," "coins converted to cash," "bonus coin," "earning app," "huge cashback," "discounts," "free cash," "get rewards," "rewards system," "easy to use," "simple interface," "user friendly," "smooth navigation," "clean design," "better interface," "quick support," "prompt resolution," "resolved the issue," "zero convenience fees," "digital gold," "QR code," "all-in-one app," "NCMC card recharge," "bus ticket booking," "train ticket booking," "wallet option," "dark mode," "app lock," "custom UPI id," "trusted," "safe," "genuine," "no problem," "excellent," "satisfied," "better than other UPI apps," "compare to other apps"
Personal Finance Manager (PFM): Look for keywords like money manager, expense manager, daily expenses, spending, spend, budget, budgeting, savings, finances, keep track, monitor, dashboard, export data, recoverable option, transactional messages, or data.
Home Loans: Look for keywords like "quick approval/sanction," "loan rejected, property not approved," "documentation hassles," "disbursement delay," "high interest & fees," "service quality," "foreclosure / part-payment issues."
Loan Against Property (LAP): Look for keywords like "mortgage loan," "collateral," "property undervalued," and "surveyor delay."
Credit Cards: Look for keywords like "annual fee," "rewards," "card declined," "fraudulent transaction."
Digital Gold: Look for keywords like "smooth purchase/sale," "sale or withdrawal not credited," "pricing or tax concern," "no / slow support," "app shows wrong values or errors," "allegations of fraud.",” gold”.
Health Insurance: Look for keywords like "claim rejection," "claim settlement," "pre-authorization," "cashless hospitalization," "network hospitals," "sum insured," "premium amount," "policy renewal," "waiting period," "OPD coverage," "maternity benefits," "critical illness," "pre-existing diseases," "no-claim bonus," "portability," "co-pay," "deductible," "hospital bills."
Mutual Funds: Look for the keywords like “Investment”,” AUM”,” NAV”, "returns” “Net Asset Value”, Asset", "Expense Ratio", "AMC (Asset Management Company)", "AUM”, (Assets Under Management)", "Benchmark", "SIP (Systematic Investment Plan)", "Lumpsum", "Rupee Cost Averaging", "Compounding", "STP (Systematic Transfer Plan)", "SWP (Systematic Withdrawal Plan)", "Equity Fund", "Debt Fund", "Hybrid Fund", "Index Fund", "ELSS”, “(Equity Linked Savings Scheme)", "Lock-in Period", "Redemption", "Exit Load", "Portfolio", "Risk-o-meter", "CAGR”, “Compounded”,” Annual Growth Rate", "Growth Option", "Dividend (IDCW) Option", "Scheme Information Document (SID)"
Generic: The review is too vague to be categorised as a personal loan or cross-sell. The language is general and unspecific (e.g., "Bad app," "Worst experience").
 If the review does not clearly mention any product-specific keywords (Personal Loan, UPI, Credit Card, Digital Gold, PFM, Home Loan, LAP, Insurance, etc.)
AND
the text is too vague, generic, or complains/praises the app without referencing any specific product or flow,
THEN classify the Product as Generic.
Examples of vague / generic reviews
“Worst app ever”
“Bad experience”
“This app is useless”
“Very good app”
“Third class service”
“Not working properly”
“App is slow”
“Excellent app”
“I don’t like this app anymore”
Do NOT assume Personal Loan or UPI unless it is explicitly mentioned or strongly implied.
If more than one product is mentioned, list them separated by a comma (e.g., Personal loan, UPI, Credit Cards).

Sentiment Analysis (Step 2):
Analyse the overall tone and emotion of the review to determine the sentiment.
The sentiment must be one of the following: Positive, Negative, or Neutral.
If someone mentions both Positive and negative about the brand, then understand if it is more towards positive or negative and accordingly tag a more positive as Positive, a more negative as Negative or an equally balanced positive and negative as Neutral.
Conditional Theme Classification (Step 3):
Condition A (for all products): If the product is a Personal Loan or any other product besides UPI (e.g., Home Loan, PFM, Credit Cards, etc.), use the Personal Loan & Cross-sell Theme List below.
If the Reviews Sentiment is Positive Then pick the Themes from the Positive Themes List 
If the Sentiment of the Review is Negative then pick the Themes from the Negative Themes List
If the Sentiment of the Review is Neutral then Pick the theme from the Positive and Negative Both themes list
Also, read and understand the themeDescription for all the themes before assigning
Additional Strict Conditions
Do NOT tag “Easy Repayment” unless the user explicitly says repayment was easy/smooth/seamless. Saying “I paid on time” does NOT qualify.

Positive themes ONLY when sentiment is Positive or Neutral.
Negative themes ONLY when sentiment is Negative or Neutral.
If the sentiment is Positive use Positive Themes List only
If the sentiment is Negative use Negative Themes List only
If the Sentiment is Neutral use Both Positive and Negative Themes
If the user says something positive and neutral in the review, put them under positive sentiment. And if a user says something negative and neutral in the review, put them under negative sentiment

Use “Misleading Loan Offers” only when the user explicitly says the loan offer shown was different from what they actually received. Do NOT use it for generic fake app complaints.

Use “Loan rejected at reapplication” ONLY when the review clearly mentions reapplication, second/third attempt, top-up, or previous successful loan followed by rejection. Otherwise use “Loan Application got rejected”.

Use “Aggressive Loan Offering” only when the review mentions repeated calls/messages/WhatsApp advising to take loans — NOT for recovery calls.

“Fraudulent company / Fake app” should be used ONLY when user explicitly says: fake, scam, fraud, cheating, etc.

**Correctly identify the product:

UPI = UPI payment, coins, cashback, recharge, bill, transfer(not disbursal)

Personal Loan = loan, EMI, approval, disbursal, CIBIL**

Use “Faced network/technical issue” only for vague technical issues. If the issue is specific (like button not working), classify under the specific issue.

Apply “Loan not closed after paying EMIs” only when user says status not updated after repayment.

**Do NOT mix “Too Many Calls” with “Unethical recovery practices”.

Too many calls = sales/support

Unethical = abusive/harassing recovery calls**

If no theme precisely fits, use “Generic – Negative” or “Generic – Positive”alone depending on the situvation

Digital Gold themes should be used ONLY for gold-related keywords: gold not credited, gold withdrawal, gold mg/grams, gold wallet.
If the review not mentioned 
If other themes are present, neither Generic - Negative nor Generic - Good Themes should be included.
If both Easy/Seamless/Hassle-free Process and Quick/Fast Process appear in the same response, ** assign only ONE category**.
Use weightage to decide which category to assign.
Weightage depends on:
Which theme is mentioned more times
Stronger emphasis (e.g., “super fast”, “extremely easy”
Context focus (speed vs simplicity)
Easy/Seamless/Hassle-free Process → Use when the focus is on smooth, simple, easy, convenient user experience.
Quick/Fast Process → Use when the focus is on speed of approval, verification, or disbursal.
Do NOT assign both categories together.
Choose the one with higher emphasis/importance in the user's statement.
Instant Loan Approval vs. Quick Loan Disbursal — Guideline
Use Instant Loan Approval only when the user talks specifically about the loan being approved instantly and does not mention the loan amount being credited.
Use Quick Loan Disbursal only when the user talks about the loan amount being credited/disbursed quickly and does not mention approval speed.
These two themes cannot appear together because they refer to different stages of the loan process. Select the theme strictly based on what the user is describing:
If the user highlights fast approval, use Instant Loan Approval.
If the user highlights fast credit/disbursal, use Quick Loan Disbursal.

**These two themes are mutually exclusive:
“Contacted Customer Support but Issue Not Resolved”

“Unable to connect to customer care / No Customer Support Helpline Number”
 Only one can be assigned based on the customer’s situation. Never return both together.**
The theme “Good for Personal Loans” is an exclusive theme and must always appear alone.Do not combine it with any other theme.If the statement matches this theme, return only this theme and nothing else.
The theme “Good UPI App” is an exclusive theme and must always appear alone.Do not combine it with any other theme.If the statement matches this theme, return only this theme and nothing else.

Specific Theme Guidelines:
Theme selection: 
**You must assign themes to the customer statement using the rules below:
Read the customer statement carefully and compare it against every available theme.
A theme must be selected only if the meaning of the statement matches the theme’s definition.
Check every theme thoroughly.
If the customer is clearly referring to multiple different aspects, return all matching themes, separated by commas.
If two themes are very similar, select only the one that is the strongest and most accurate fit based on the customer’s intent. Choose both only when the customer is clearly talking about both separately.
Never assign themes based on keywords alone. Themes must match the intent.
Apply co-existence rules strictly (for example: approval themes cannot be mixed with disbursal themes unless the user talks about both events).
If no theme matches, return “Uncategorized”.**
Trusted Loan App: This should appear only if in the statement the sense of trust is actually being implied, else it shouldn’t come out as a theme. This theme should be applied “only” when the statement strongly indicates trust, safety, security, reliability, or transparency. You should be 100% sure that a particular statement has something being said which indicates trust, safety, security, reliability, or transparency and only then should this be given as a theme. - Look for phrases that mention:
Direct Trust: Words like "trusted," "reliable," "genuine," or "honest," as well as any direct mentions of "safety" or "security."
Transparency/No Fraud: Mentions of "no hidden charges," being "transparent," or the app "delivering on promises."
Example of statements which has Trust as a theme:
"Moneyview is a trusted app with no hidden charges, and I felt safe using it." -> Trusted Loan App
"Unlike other apps, Moneyview is genuine and transparent, making it my go-to." -> Trusted Loan App
Good secure and Safety App thank you Money view
i don't review a àpp easily, but this app is 👌🏻🪄 100% trustable till the date, i don't know tomorrow what happen about this app, but i use in one year not more scam in this app 🔥, 5/5 star for this app
Very good,fast and secure app.it gives cashback everytime on payment. please enable security lock on the app for protection
I had a great experience. The loan process was quick, simple, and fully online. The amount was disbursed instantly, and there was full transparency in terms and conditions. Perfect app for urgent financial needs. Highly recommended!
excellent performance and reliability in this world is service and reliability
Generic - Good: This theme must not be applied if any other theme applies to the review. Use this theme only as a last resort. It is reserved for short, positive, and generic statements that do not contain any specific details to fit another theme. If “generic-good” as a theme appears, then no other theme from the list should come along with it. 
Example Application:
"Very nice loan aap. I got my second loan yesterday. Thanx Money view." -> Generic - Good
"Thank You Very much & Very helpful, in Moneyview." -> Generic - Good
"Good/ nice/ best loan app" -> Generic - Good
Easy to use app: This theme should be applied specifically when the user's feedback is about the ease of navigating and using the application's interface, not the loan process itself. Look for keywords and phrases like "good navigation," "no difficulty using the app," or "easy to use."
Example Application:
"The app has good navigation and it was a very good experience." -> Easy to use app
"I had no difficulty using the app, it was very easy to use." -> Easy to use app
"The loan process was easy, but the app itself is a bit complex to navigate." -> Easy to use app would not be applied here.




For all products:
Personal Loan / UPI/Generic/ PFM/ Digital Gold/ Home loan/ LAP/ Credit cards etc
You are an expert theme categoriser for customer feedback. Your task is to analyze a given statement and identify ALL relevant themes from a predefined list. A single statement can have one or more themes. 
Below is the format for the Theme with Description for your understanding
But you have to pick only Theme in the output, not the Description.
Here are the Positive themes you MUST choose from: 

Theme
Description
[NET] Speed & Easy Loan Approval

Quick/Fast Process
User finds the loan application process fast, theme should appear only when quick loan disbursal/instant loan disbursal is not mentioned.
Instant Loan Approval
Loan approval happens instantly. (Only if a user is talking about Approval and not disbursal/credit)
Quick Loan Disbursal
Loan amount credited quickly after approval, Their amount was credited/disbursed quickly/fast/instantly. In case the consumer talks about approval and not amount being credited or disbursed, then this shouldnt appear.
Fast KYC Verification
KYC verification is quick and seamless. (Only when KYC being fast/quick/instant is being mentioned) Do not mention if KYC is not mentioned
[NET] Easy process/ less documentation/ Online loan

Less Documentation
Minimal/less number of/No documentation required for loan application. (If a customer talks about quick documentation, this theme should not be picked up)
Online/Paperless Process
Loan journey is completely digital and paperless. No physical documentation upload required, entire journey was completed online.
Easy/Seamless/Hassle-free Process
Overall loan process feels easy and convenient. If user mentions quick/fast/instant, then this code should not appear. Should only appear when ease, easy, seamless, no hassle, smooth, etc are mentioned
[NET] Payment UPI features

Good for UPI Transcations
The user says the app is good, useful, smooth, or reliable for UPI payments or UPI-based transactions.Should only appear when:The user explicitly mentions UPI transactions, UPI payments, or UPI performance being good.Feedback highlights smooth, fast, easy, or reliable UPI usage.Should not appear when:The user is talking about general payments without mentioning UPI.The user only talks about QR scanning or wallet features without referring to UPI.The mention is related to loan payments but not UPI.
Fast & seamless transactions
Transactions occur instantly and reliably. User should not talk about loans, else it should not pick up this theme. Determine properly if the seamless transaction is being mentioned for loan disbursal or UPI transaction and only then predict the product
Good App for Recharge/Bill Payments
Recharge and bill payment features work well. If 'payment' term appears without the mention of bills, do not pick this code.
[NET] Trust & Security

Trusted/Reliable App
Users trust the app's functioning, security and reliability. If users talk about secure along with transactions, then do not use this theme. Can mention when user talks about the app being genuine too.
Secure Transactions
Transactions feel secure and protected. If transaction is not referred to as secure, this theme should not be picked up. Should be picked up only when the product is UPI and Digital Gold, should not be picked up with any other product.
Maintains Transparency / Clear T&C
Terms and conditions are clear and transparent. Should not be picked when user talks about no hidden charges/extra penalty charges. Should only be picked when transparency and terms and conditions are mentioned



[NET] Good customer support/ service

Good/Helpful Customer Support
Support team helps effectively. This theme defines the satisfaction of users, not the speed
Quick Response from CS
Support responds quickly. This theme captures the speed of response, not satisfaction.
[NET] Better than competition

Better than Competation
App performs better than alternatives or says best without mentioning any alternative. Should not be picked up at the generic positive statements like 'good,amazing, nice, etc'. Should definitely have "best" or should be comparing with another app - "better than other app, etc".
Only App to Approve Loan
Loan approved here when others rejected, should only talk about Approval and not disbursal.
[NET] Additional benefits/ terms

Loans for Every Need
App provides loans for multiple needs. Should not appear for emergency/urgent/business use. Should appear only when talking about - 'app being useful for multiple needs'
Loan Eligibility at Low CIBIL
Loans provided despite moderate CIBIL score. Use it only when the sentiment is positive and taks about receiving or being eligible for the loan despite having a moderate/low CIBIL score.
NOC Readily Available
NOC issued quickly and proactively. Should appear only when talking about NOC.
Shows Accurate CIBIL Score
App shows correct CIBIL information
Manages Expenses & Budget
Helps track or manage expenses
[NET] Beneficial for repeat users

Loan Amount Grows After Each Loan
Loan limit increases with repeated avail of loans from same platform. Should not appear when user only talks about high loan amount. Should only appear when user mentioned the increase in loan amount in the subsequent loan from the lender.
Eligible for another loan soon after closing one or shorter freezing period
Eligibility restored quickly after closure of previous loan or after a very short cool off period. Should not appear when user talks about eligibility without the mention of repeat loan.
[NET] Good collections and recovery practice

Good Recovery Practices
Recovery/operations/collections staff behave politely and professionally. Should not appear for customer support politeness. This code refers only to the EMI recovery practices.
[NET] Attractive Loan Offer

Higher loan amount than others
Offers higher loan amount than competitors. Should only come when compared to other brands or if someone says that it offers the best loan amount in the market.
Interest Rate is Low/Good
Interest rate offered is favorable, low, good,etc
No Extra Charges / Zero Platform Fees
No hidden charges are applied or extra charges applied without users' knowledge. Should not appear if user talks about low processing fees. Should only appear when hidden/extra charges are mentioned
Low Processing Fees
Processing fees are affordable, low, good, nice, etc
Discount on Processing Fees
Users receive discounts on processing charges. This code should appear only when discounts/offers/promocode/waive off are mentioned for the processing fees. Should not confuse with Low processing fees.
Provides good tenure options
Should only appear when user appreciates the tenure
Offers good loan amount
Should not appear when user says best loan amount. Should appear only when user mentions that this platform provides good / high loan amount. If comparison with other brands is happening, then this code should not appear here.
Good loan offers provided
Users like the loan offers, not specifying any specific feature of the offer they like (like EMI, Interest rate, processing fee, Loan amount, etc). This theme should appear only when user mentions generic - loan offer and not feature of the loan offer.
No Pre-closure Charges
No charges for early closure
[NET] App Usability & Experience

Easy to Use App
App is simple and intuitive to use. Should not appear when user talks about friendly UI. Should only appear when the statement is generic and positive and talking only about the App and not the UI or product.
User-Friendly Interface
User interface is clean and easy to navigate. Should appear only when talked about interface or design or colours of the app.
Seamless Cross-Device Sync
App syncs across devices smoothly
Beginner-Friendly
New users can use the app easily. Should not appear when repeat loan is mentioned. Users should not talk only about the design or navigation. this theme should appear only when there is a mention of app being friendly/useful for a first time/beginner/new user
Easy to Register/Login
Signup, login, registration is smooth/easy
Timely notifications are helpful
Notifications are timely and useful. Should appear only when talked about notifications (sms/push notifications/emails) and not app being helpful. Users should not be talking about customer supoort or sales calls
[NET] App rewards/ Referral and discounts

Good cashback and rewards
Users appreciate the rewards or cashback benefits they receive. This includes general mentions of good cashbacks/rewards as well as rewards earned specifically for using the app or product. Should be applied when users highlight receiving value through reward points, cashback, or app-usage-based rewards.Do not confuse with conversion to cash and cashbacks.
Rewards-to-cash conversion
Reward redemption/conversion to money/cash is smooth/easy/likable, etc. Should not appear when conversion to cash or money is not mentioned or when they talk generally about rewards
Referral rewards are good
Referral system is rewarding. Should not appear when referral is not mentioned. This code specifically talks about the rewards received from referrals
Likes Coupons & Offers on the App
Users appreciate coupon deals. Should not appear when talking about processing fee here.
[NET] CIBIL Benefits

Helps Increase CIBIL
Only mention when Users see improvement in their credit score, or have a perception that this will help in increasing the CIBIL score
[NET] Easy flexible repayment

Easy Repayment
EMI Repayment experience is smooth/seamless/easy
Flexible EMI Payment
EMI payment are flexible. This code mostly talks about flexibility in repayement/EMI amount. Should appear only when user talks about flexible amount and not the duration.
Flexible Repayment Date
Repayment dates can be adjusted. Should appear only when user talks about the benefits of having an option to pay manually/at their own preferred time
Can Suggest to peers
User willing to recommend app, asking to download/use the app.
[NET] Required additional benefits/features

Require more help/guidance/support
This theme should appear when user suggests the requirement of help,guidance,support. But it should not appear when they mention negative things about customer support.
Require additional features
This theme should only appear when user suggests or informs the need for additional features like dark mode, UPI id change option, adding credit cards to payments, wallet feature, silver product, etc
[NET] Empowering community growth

Helpful for Students
App helpful for student needs
Supports Small Businesses
Useful for small/medium businesses
Supports Salaried Individuals
Useful for salaried users
Useful for short term loans
Should only appear when user specifically mentions that app is good for short term loans
[Independent Net's]

Good app for Investment and other financial services
Investment features work well. Do not use when the product is UPI.
Good for Personal Loans
Generic statement for App works well for personal loans. Shouldnt appear when user talks abiyt other features of Personal loans. Should only appear when user says the app is good for personal loans generically and not specifically. Should only appear when other codes are not appearing
Likes Celebrity Brand Ambassador
Brand ambassador creates positive appeal (Ranbir Kapoor, MS Dhoni)
Helpful during emergency/ Need
Provides help during emergencies or when urgent need arises
Multiple Services
Multiple useful features and services all in one app. Should not appear when user talks about just one service or product they like. Should only appear when user feels that one app has many/multiple useful products/features
Generic - Good
General positive remarks
Uncategorized
Unclear/ Uncategorized feedback (which are in no way related to finance or service)
Good UPI App
Generic statement for Good/Best/Nice UPI App.If the statement matches this theme, return only this theme and nothing else.
Zero Down Payment
User highlights that no upfront payment was required while taking the product or service. This theme should appear only when the user explicitly mentions not having to pay any initial amount or zero down payment.
Spam-Free Loan App
Users do not receive spam communication or do not receive too many calls.





Here are the Negative themes you MUST choose from: 

Theme
Description
[NET]Loan got rejected

Loan Application got rejected
Loan rejected during first application. Should not appear when user talks about top ups/repeated loans/reapplication, etc
Loan rejected at reapplication
Loan rejected during reapplication attempt. Should only be mentioned when user talks about top ups/repeated loans/reapplication, etc. Should only appear when user mentions that he already took one loan with thsi brand previously, but on taking another loan, it is getting rejected.
Loan Rejected After Approval / Process Completed
Loan was approved but later rejected after user completed the process or after document upload. Should not appear when rejection is mentioned without approval. It does not mean approval in previous application or attempt and reject in new application. it should only appear when user mentions that in the same application, his application was approved but after completing further process of the same application it got rejected.
Loan rejected inspite of good CIBIL score
User denied loan despite high CIBIL. It should only appear in a negative statement.
[NET]Delay in the process

Facing some issue with loan application/Stuck in the process
Application process error. If a user got some error, faced any issue or mention he is stuck or unable to proceed further until help/resolve. This code should only appear when user talks generally about being stuck without stating what the exact issue was and not specifically about an issue in detail. If they mention the issue in detail, this code should not appear. Should not appear when users talk about lag in app.
Difficulty competing in KYC verification
KYC process fails repeatedly or facing issue at KYC stage. Should only appear when any issue with KYC is mentioned
Loan Amount Not Disbursed / Delay in Receiving Amount
Should only appear when user mentions delays in disbursals/credit of loan amount. Should not appear when user mentions issue with process.
App performance/lag issues
App is slow or laggy. Should not appear when user talks about being stuck in application. It should only appear when user mentions app performance issue or lags in app.
Account Blocked
User account is blocked unexpectedly
[NET]Issues with the customer care

No Response from Customer Support
Support didn't respond. When users mention that called/emailed/reached out to customer support but no one responded.
Unable to connect to customer care/No Customer Support Helpline Number
Support unreachable. This code should appear when user says that there was no way to contact our customer support. It should not appear when user says that they tried calling/emailing/reaching out to customer supoort. This theme is only for those users who had no means to contact the customer support
Contacted Customer Support but Issue Not Resolved
Support replied but didn't fix problem. This theme should not appear when users mention that they could not connect with the customer support. This should only appear when users mention that they connected with cutomer support but they could not resolve their issue or were unhelpful.
Rude Customer Support Staff
Support staff behaved rudely
Too Many Calls from Customer Care
Excessive Customer support or recovery calls. This code should not appear when users talk about sales calls.
[NET]Issues with the offer

Interest Rate is Very High
Interest rate feels unaffordable or high. Can also appear when user suggests lowering the interest rate. Interest rate can also be mentioned as interest or ROI.
Requires More Loan Amount
User wants higher loan limit and is unhappy with the current loan amount offered. Can also appear when user mentions that he got a higher loan amount at some other platform along with competition is better. Should not appear when user mentions loan amount not increasing in subsequent loan.
Loan amount not increased in subsequent loan
Only mention when users mention not getting as much or more amount than their previous loan at the same platform. Should not appear when users generally talk about not liking the loan amount offered.
High Processing Fees
Processing charges are too high
[NET]Technical issues

Unable to View Offer on App
Offer page does not load. Theme should appear when users mention mention that they are/were unable to see the offer page due to some issue. Should not appear when users talk about not liking the offer.
Faced network/technical issue on the app
Users face network or technical glitches. Should NOT appear when users specify the exact technical issue they faced in detail, then it should go under the specific issue they faced. this should only appear when users talk in general about facing some technical issue or network issue. eg. if a user says that he was unable to check his balance loan amount, it should generate a new code instead of giving this theme because this theme should only appear when user is not specifying a code.
Unable to register/ login
Signup or login fails due to some issues like - unable to access account, lost email, unable to verify account failure, dont remember login email or facing issue in registering
Issue with Account Deletion
User cannot delete account. Facing issue in deleting account
OTP / Verification Failure
OTP not received or fails when input
Unable to verify Income via Net Banking
Income verification via net banking fails or does not complete, people saying they do not have net banking option should not appear in this theme
Unable to see EMI details
The user is unable to view EMI information such as EMI amount, due date, schedule, breakdown, or repayment details within the app.Should only appear when:The user explicitly says they cannot see EMI details.Mentions like “EMI details not visible,” “unable to check EMI,” “EMI info missing,” or “EMI page not loading.”The issue is specifically about visibility or availability of EMI information.Should not appear when:The user talks about high EMI, EMI amount issues, or EMI not deducted.The user mentions repayment problems unrelated to viewing EMI details.The problem is general app loading or login failure that prevents seeing anything, not specifically EMI details.
[NET]Issues with the CIBIL Score

CIBIL score affected
Pick when user claim their CIBIL dropped. Don't use when drop is due to external reasons not app-related.
Shows innaccurate CIBIL Score
Displayed CIBIL score is wrong. This should not appear when user mentions CIBIL score got reduced due to some issue with loan application/closure. Should only appear when user mentions CIBIL is displayed wrong on the app/credit tracker.
[NET]Trust issues/Fraud chances

Fraudulent company/ Fake app
User suspects app is fraudulent. Use when user calls app scam/fake. Don't use when user is simply unhappy.
Trust issue
General distrust in app. Use for general distrust. Don't pick for transparency complaints unless distrust is explicit.
Lack of Security Features
App lacks important security measures/features. Don't use when user praises security elsewhere.
Privacy concerns
User feels data privacy is compromised. Use when user concerned about data misuse. Don't use when permissions are unrelated to privacy concerns
Annoying Permission Requests
App requests too many permissions. Only mention when user mentions he is annoyed/irritated with too many permissions. Should not appear when permissions are related to trust issues or privacy concerns.
Lacks Loan Process Transparency
Loan process transparency is insufficient. Don't use for hidden fees if transparency isn't mentioned.
Concerns About Bad Reviews
Users doubt the app’s credibility due to seeing bad reviews. Don't use for their own negative experience.
[NET]Post Disbursal Delays/Issues

Loan not closed after paying the EMI's
Loan remains open after full payment
Delay in receiving the NOC
NOC not issued on time after closure of loan
Unethical loan recovey practices
Recovery staff behaviour is bad
[NET]App rewards/ Referral Reward Low/Not received

Inconsistent / low / no rewards / require more rewards
Rewards are too low / inconsistent / abscent, want more rewards/cashbacks. Do not mention if user is talking about not receiving rewards which were promised.
Rewards not received
Promised rewards/cashback are missing. Do not mention at general unhappiness of rewards
Referral bonus not Received
Referral rewards not credited. Do not mention at general unhappiness of rewards
Misleading Reward Advertisement
Advertised reward differs from actual or expected reward. Do not mention at general unhappiness of rewards
[NET]Issues related to payments

EMI Deducted Twice
EMI deducted more than once
Complicated Repayment Process
Repayment flow of loan is confusing
Can't make manual EMI payment; do not prefer auto-deduction
Unable to Make an Manual EMI Payment/Manual payment option missing/Do not want money to get deducted automatically
Unable to change Auto debit bank details
Auto debit Bank details cannot be updated/changed
[NET] Others_Negative

Too Many App Updates
Too many alerts and notifications
Misleading Loan Offers
Loan offer is not as originally shown in advertisement or after login
[NET] Issues with Digital Gold

Incorrect Investment Amount Shown
App displays the wrong investment amount. Should only appear when product is Digital Gold
Gold sale Withdrawal Not Credited
Gold redemption money not credited. Should only appear when product is Digital Gold
Poor return on investment
Low investment returns. Should only appear when product is Digital Gold
[Independent Net's]

Foreclosure Restrictions
Restrictions on foreclosure cause issues like foreclosure not allowed or unable to foreclose loan. Should not appear when user states issues post foreclosure
Competition is Better
User prefers competing apps. Only mention when user is comparing one app to another. Should not appear when user mention that this app is better than other. Should appear only when user mentions that some other app is better than this app
EMI Debited but Not Reflecting in App
The customer says the EMI was already debited → payment successful.But it is not updated / not visible in the app → app sync/reflect issue.No other themes apply (not a repayment failure, not a double debit, not an overdue issue).
Poor UI
Interface feels confusing or outdated
Aggressive Loan Offering
Too many loan sales attempts. Should not appear when user says too many calls from customer support. This code specifically talks about Sales calls
Penalty/Extra/Hidden Charges
Hidden or unexpected charges applied on the loan offer/EMI, etc
Faced Issue with UPI payment/ failures
UPI payments fail often. Should not appear when user generally talks about unhappiness with UPI's features. Should only appear when the product is UPI.
Unhappy with the offer
User dissatisfied with loan offer terms. Its focused more with generic - unhappy with the offer and shouldn't come along with other specific reasons like loan amount, ROI or Proc fee
Generic - Negative
General negative remarks
Uncategorized
Unclear/ Uncategorized feedback (which are in no way related to finance or service)
Unable to add bank account
User reports that they are unable to link or add their bank account in the app. This should appear only when users explicitly mention issues or failures while trying to add a bank account.
















Rules for Creating New Themes
Strict Uniqueness: If a statement contains a theme not on the predefined list, you must create a new one. This new theme should be an entirely new concept and not similar in meaning to any existing theme.
Be 99.9% confident that no existing theme is a closer match before creating a new one. If a closer theme exists, use it instead.
Repository of Themes: You must maintain a repository of all themes you have created. Before creating a new theme, check this repository to see if a similar theme already exists. If so, use the exact phrase from the repository to avoid creating duplicates with similar meanings.
Format: A new theme must be a precise phrase of 4 to 5 words maximum. It must also be followed by a space and a pipe character ( |) at the end.
No Themes: If a statement does not fit any existing theme and a new one cannot be created, categorise it as "Uncategorized."






Output Format
The output must be a single line in the following strict format:
[Product Type(s)], [Sentiment], [Theme(s)]
Product Type(s): Comma-separated list (e.g., Personal Loan, PFM).
Sentiment: One of Positive, Negative, or Neutral.
Theme(s): Comma-separated list of all relevant themes. New themes must end with ‘ |’.
Examples
Example 1 (Personal Loan):
Statement: "My loan application has been stuck on the verification page for three days, and no one from customer care is responding. Very frustrating."
Output: Personal Loan, Negative,Facing some issue with loan application/Stuck in the process,Contacted Customer Support but Issue Not Resolved
Example 2 (Personal Loan):
Statement: "I paid my EMI on the 1st of the month, but they still deducted it again on the 5th. Now they are not refunding my money."
Output: Personal Loan, Negative, EMI Deducted Twice, Not refunding the 2nd EMI debited
Example 3 (UPI):
Statement: "I love this app for making payments! It's so fast, and the cashback rewards are great. I also got a bonus for referring to my friend."
Output: UPI, Positive, Fast & seamless transactions,Good cashback and rewards,Referral rewards are good
Example 4 (UPI):
Statement: "I had a payment failure and the customer support was totally unhelpful. They didn't even try to resolve the issue."
Output: UPI, Negative,Faced Issue with UPI payment/ failures,Contacted Customer Support but Issue Not Resolved
Example 5 (Multiple Products):
Statement: "I tried to use the app to check my credit score, but it kept freezing. The money manager shows my daily expenses twice, making my budget look completely off. Very buggy."
Output: PFM, Negative, App performance/lag issues
Example 6 (Generic):
Statement: "Useless app."
Output: Generic, Negative, Generic - Negative
Example 7 (Personal Loan - Positive):
Statement: "Got my loan in less than 24 hours. The process was completely online and super easy. I'm very happy."
Output: Personal Loan, Positive, Quick Loan Disbursal,Online/Paperless Process, Easy/Seamless/Hassle-free Process
Example 8 (UPI - Positive):
Statement: "This is the best UPI app I've used. Payments are always successful and I love that I can recharge my mobile directly from here."
Output: UPI, Positive,Good for UPI payments,Good App for Recharge/Bill Payments
Example 9 (Generic - Positive):
Statement: "Awesome app, the UI is so smooth and clean. Feels very secure."
Output: Generic, Positive, User-Friendly Interface, Secure Transactions
Example 10 (Digital Gold - Negative):
Statement: "My recent gold sale shows as processed but I haven't received the money in my bank account. Customer support is not helping at all. This is a scam and you guys are a fraud company."
Output: Digital Gold, Negative,Gold sale Withdrawal Not Credited, Contacted Customer Support but Issue Not Resolved, Fraudulent company/ Fake app

Example 11 (Home Loan - Negative with predefined themes):
Statement: "The application for my home loan was sanctioned quickly, but the surveyor delayed the inspection, and the interest rate was much higher than promised."
Output: Home Loans,Negative,Interest Rate is Very High, Unhappy with the offer







Example 12 (Personal loan - Positive with predefined themes):
Statement: "good apps and easy process for loan system. thank you moneyview"
Output: Personal loan,Easy to use app,Quick/Fast Process
Example 13 (Personal loan - Negative with predefined themes):
Statement: "It's good but try to better easy to refer and kyc verification is too hard and improve cash back differently"
Output: Personal loan,UPI,Difficulty competing in KYC verification,Wants Better Cashback System 
Example 14 (Personal loan - Positive with predefined themes):
Statement:“Money View is a great app in itself. It does not complicate the process and the money is transferred to your account quickly with less documentation. Unlike other apps that only check if you are eligible for a loan or not. A great friend Money View is a good app”
Output: Personal loan,Easy/Seamless/Hassle-free Process,Quick Loan Disbursal,Less Documentation,Better than competition,Helpful during emergency/ Need
Example 15 (Personal loan - Negative with predefined themes):
Statement:“money view app do not force close the loans don't customer care number lost of problems are there in money view app”
Output: Personal loan,Contacted Customer Support but Issue Not Resolved,Foreclosure Restrictions  

Example 16 (Personal loan - Positive with predefined themes):
Statement: “I really appreciate this app when I need an urgent money the money view always stands with me and for my business”
Output:Personal loan,Helpful during emergency/ Need,Trusted/Reliable App,Supports Small Businesses
Example 17 (Credit card,Bill payment - Positive with predefined themes):
Statement: “Great use of credit card and bill payment getting good cashback”
Output:Credit Cards,Bill payments,Good App for Recharge/Bill Payments,Good cashback and rewards
Example 18 (Multiple products):
Statement: “I applied for the credit card and got my card delivered in just 3 days great app and i got the credit card even after my personal loan rejected”
Output:Credit card,Personal loan,Neutral,Easy/Seamless/Hassle-free Process,Loan Application got rejected
Example 19 (Multiple products):
Statement: “I have applied for the Home loan but still did not received any update very poor service and when i called their customer care they don’t have a knowledge on this and my EMI for personal loan also got debited twice from the account”
Output:Home loan,Personal loan,No Response from Customer Support,EMI Deducted Twice



Example 20(Personal loan - Negative with predefined themes)
Statement: “Again, again my PAN card problem. Last time I logged another email, and I forgot email bus my new nahi le rhaa hai or mai lone wpas nahi le paa rhaa hoo 6 month se or iski costumer ko call bhi nahi lagta don't use this app guys please”
Output: Personal loan,Negative,Unable to register/ login,Unable to connect to customer care/No Customer Support Helpline Number
Example 21
Statement: “The NACH mandate is active, but the loan is not processing. I tried to contact, but no one picked up”
Output: Personal loan,Unable to connect to customer care,Facing some issue with loan application/Stuck in the process
Example 23
Statement: “Amazing Fraud...In the beginning, it will show you that you are eligible for a decent amount you can get immediately, but in the end, it will reject your application, and you will be disappointed. Don't scam people”
Output: Personal loan,Loan Rejected After Approval / Process Completed,Misleading Loan Offers
Example 24
Statement: “I had applied for a loan. The system was unable to verify In by net banking, and was unable to read the statement PDF. Mailed multiple times, but got no response yet. Useless app”
Output: Personal loan,Negative, No Response from Customer Support, Unable to verify Income via Net Banking
Example 25
Statement: “The very worst service provider. We sent a technical issue email 12 days before, but there has been no response. Worst customer care service. We uninstall the application.”
Output: Personal loan,Negative,Faced network/technical issue on the app,No Response from Customer Support
Example 26
Statement: “Previous loan cleared.. But the next loan was rejected.. Such a bad loan app ever.. Why comming calls from this app? Stupid things.”
Output: Personal loan,Negative, Loan rejected at reapplication, Too many calls from customer care
Example 27
Statement: “Useless app, never take a loan from this kind of app. My loan was approved before one Call for EMI is also done, but there will be no amount disbursed in my account. Totally disappointed.d No customer support.t No one is there to listen to you. Totally a waste of time. Never go with this kind of app”
Output: Personal loan,Loan Amount Not Disbursed / Delay in Receiving Amount,Contacted Customer Support but Issue Not Resolved
Example 28
Statement: “Very bad experience Initially they say that you can close the loan after one month but nothing is happening, you have to pay in six months and then you can close the loan”
Output: Personal loan,Negative,Foreclosure restrictions

Example 29
Statement: “Pathetic customer care service. Trying to connect with customer care to change my auto debit bank details for one bank to another there is no response from team.”
Output:Personal loan,Negative,Contacted Customer Support but Issue Not Resolved,Unable to change Auto debit bank details
Example 30  
Statement: “Worst Experience.I won't recommend anyone..if 1 emi is missed they will Harass you with more than 100 calls per person per day to you and references.
Output:Personal loan,Negative,Unethical loan recovey practices
Example 31    
Statement: “Totally bad service provide. The customer service not proper. As they told us about noc received within 14 days after all due payment but still it not received.”
Output:Personal loan,Negative,Delay in receiving the NOC,Contacted Customer Support but Issue Not Resolved
Example 32  
Statement: “Worst App I have taken a loan pay all the Emi on time through auto Debit after that They have decreased My Credit limit and My loan also showing active which hugely effected My Credit Score”
Output:Personal loan,Negative,CIBIL score affected,Loan not closed after paying the EMI's
Example 33
Statement: “very fast and secure the app....no 0% cash payment....”
Output: Two-wheeler loan, Neutral, Quick/Fast Process, Trusted/Reliable App, Zero Down Payment
Example 34
Statement: “Google Play redeem code nahi banta hai”
Output: Generic, Negative, Lacks Google Play redeem code option
Example 35
Statement: “Not account expected another 4 account 5 account aad option Probelm sol please Mujhe nya account chahiye par ye aap. De nahi Raha hai”
Output: UPI, Negative, Unable to add bank account
Example 36
Statement: “KreditBee. good service fast, so happy with smile service”
Output: Generic, Positive, Quick/Fast Process]''', 
          "\nReview: ", content
        ) AS prompt
        FROM `playstore2026.play_store_data.app_reviews` AS raw
        WHERE NOT EXISTS (
          SELECT 1 FROM `playstore2026.play_store_data.app_reviews_ai` AS ai 
          WHERE ai.reviewId = raw.reviewId
        )
      ),
      STRUCT(
        0 AS temperature, 
        400 AS max_output_tokens, 
        TRUE AS flatten_json_output
      )
    )
    """
    
    try:
        print("🤖 Starting Gemini 2.5 Flash Classification...")
        query_job = client.query(sql_query)
        query_job.result()
        print(f"✅ AI Processed {query_job.num_dml_affected_rows} new reviews.")
    except Exception as e:
        print(f"❌ AI Classification Failed: {e}")

def scrape_all_apps():
    client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    print(f"--- Starting Scrape for {yesterday} ---")

    all_new_reviews = []
    for app in APPS:
        print(f"Fetching reviews for {app['name']}...")
        try:
            result, _ = reviews(
                app['id'], lang='en', country='in', 
                sort=Sort.NEWEST, count=500
            )
            if not result: continue

            df = pd.DataFrame(result)
            df['at'] = pd.to_datetime(df['at'])
            df['app_name'] = app['name']
            
            mask = (df['at'].dt.date == yesterday) & (df['content'].str.len() >= 30)
            df_filtered = df[mask].copy()
            
            if not df_filtered.empty:
                df_filtered['at'] = df_filtered['at'].dt.strftime('%Y-%m-%d %H:%M:%S')
                all_new_reviews.append(df_filtered)
                print(f"Found {len(df_filtered)} reviews.")
        except Exception as e:
            print(f"Error scraping {app['name']}: {e}")

    if all_new_reviews:
        final_df = pd.concat(all_new_reviews, ignore_index=True)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        
        print(f"Uploading {len(final_df)} total reviews...")
        try:
            job = client.load_table_from_dataframe(final_df, TABLE_ID, job_config=job_config, location=LOCATION)
            job.result()
            print("Upload successful!")
            # TRIGGER THE AI CHAIN IMMEDIATELY AFTER SUCCESSFUL UPLOAD
            trigger_gemini_classification()
        except Exception as e:
            print(f"BigQuery Upload Failed: {e}")
    else:
        print("No new reviews to upload today.")

if __name__ == "__main__":
    scrape_all_apps()
