Grid Marketmaking

GridV2_Executor:
Orders of x levels on both buy and sell side based on Fair Price and given quantities

Config:
Fair_Price_Type: “Mid Price”
Spreads: [s1, s2, s3, s4]
Quote_amount: [Q1, Q2, Q3, Q4]
Price_Refresh_Tolerance: X%


4 Orders on both sides using Fair Price.

Fair_Price = Mid_price = (Best_Ask+Best_Bid)/2

O1 (Sell):
Price = Fair_Price * (1+(s1/100))
Qty = Q1/Price

O1 (Buy):
Price = Fair_Price * (1-(s1/100))
Qty = Q1/Price

So on for next 3 orders

If Fair_price changes more than X% then only refresh the orders


Grid Marketmaking Strategy:
Grid Executor takes care of placing grid orders. If we get any fill we need to place a Target Profit Order. We keep track of Avg Price of Fills.
If Exposure > max Exposure:
Place orders at Best Bid/Best Ask based on side

Config:
GridV2_Executor Config = {}
Target_Profit = TP%
Max_Exposure = X
Total_Balance = Y

Exposure Calculation:
We need to maintain Y balance.
Exposure = Curr_Balance - Y

Target Profit Order:
Use Order Executor with Limit Maker Order

Price = Avg_Price * (1+(TP/100)) if Exp +ve else Avg_Price * (1-(TP/100))
Qty = Exposure

Exit Order when abs(Exp) > Max Exposure:
Use Best_Price_Executor:
Qty: Abs(Exposure) at Best_Ask/Best_Bid

to_format_status:
Current Order
Profit Target Order (If greater than expose or not)
Avg price of order
Balance
Past trades (last 10, can be fetched sqllite files from data/*.sqlite)
