# BigDataProcessing_Ethereum
Analysis of Ethereum Transactions &amp; Smart Contracts


Assignment

Write a set of SPARK jobs that process the given input and generate the data required to answer the following questions. *Note, you may choose to attempt any combination of questions/tasks below that sum up to 100% or even more (the grade is capped at 100%). Generally speaking tasks in Part A, B, and C (totaling 60%) are easy, while tasks in Part D (totaling 40%) vary in difficulty to match all students. Note the 100% grade for this course project will be scaled to account for 30% out of the module total grade. *
Part A. Time Analysis (25%)

Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset.

Create a bar plot showing the average value of transaction in each month between the start and end of the dataset.

Note: As the dataset spans multiple years and you are aggregating together all transactions in the same month, make sure to include the year in your analysis.

Note: Once the raw results have been processed within Spark you may create your bar plot in any software of your choice (excel, python, R, etc.)
Part B. Top Ten Most Popular Services (25%)

Evaluate the top 10 smart contracts by total Ether received. You will need to join address field in the contracts dataset to the to_address in the transactions dataset to determine how much ether a contract has received.
Part C. Top Ten Most Active Miners (10%)

Evaluate the top 10 miners by the size of the blocks mined. This is simpler as it does not require a join. You will first have to aggregate blocks to see how much each miner has been involved in. You will want to aggregate size for addresses in the miner field. This will be similar to the wordcount that we saw in Lab 1 and Lab 2. You can add each value from the reducer to a list and then sort the list to obtain the most active miners.
Part D. Data exploration (40%)

The final part of the coursework requires you to explore the data and perform some analysis of your choosing. Below are some suggested ideas for analysis which could be undertaken, along with an expected grade for completing it to a good standard. You may attempt several of these tasks or undertake your own.
Scam Analysis

    Popular Scams: Utilising the provided scam dataset, what is the most lucrative form of scam? How does this change throughout time, and does this correlate with certain known scams going offline/inactive? To obtain the marks for this catergory you should provide the id of the most lucrative scam and a graph showing how the ether received has changed over time for the dataset. (20%/40%)

    Wash Trading: Wash trading is defined as "Entering into, or purporting to enter into, transactions to give the appearance that purchases and sales have been made, without incurring market risk or changing the trader’s market position" Unregulated exchanges use these to fake up to 70% of their trading volume? Which addresses are involved in wash trading? Which trader has the highest volume of wash trades? How certain are you of your result? More information can be found at https://dl.acm.org/doi/pdf/10.1145/3442381.3449824. One way to attempt this is by using Directed Acyclic Graphs (DAGs). Keep in mind if that if you try to load the entire dataset as a graph you may run into memory problems on the cluster so you will need to filter the dataset somewhat before attempting this. This is part of the challenge. Other approaches such as measuring ether balance over time are also possible but you will need to discuss accuracy concerns. Marks will be awarded in a scale relative to the sophistication of your solution. If you can detect simple wash trading between two participants that is worth 20 marks. If you can detect more complicated wash trading between three or four participants that is worth 30 marks. If you can detect wash trading between an arbitrary number of participants that is worth full marks. (40%/40%)

Contract Types

    Identifying Contract Types: The identification and classification of smart contracts can help us to better understand the behavior of smart contracts and figure out vulnerabilities, such as confirming fraud contracts. By identifying features of different contracts such as number of transactions, number of unique outflow addresses, Ether balance and others we can identify different types of contracts. How many different types can you identify? What is the most popular type of contract? More information can be found at https://www.sciencedirect.com/science/article/pii/S0306457320309547#bib0019. Please note that you are not required to use the types defined in this paper. Partial marks will be awarded for feature extraction and analysis. Classifying contracts based upon ERC20 or ERC721 only will be awarded minimal marks e.g. 10. I expect something more sophisticated for this. (40%/40%)

Miscellaneous Analysis

    Gas Guzzlers: For any transaction on Ethereum a user must supply gas. How has gas price changed over time? Have contracts become more complicated, requiring more gas, or less so? How does this correlate with your results seen within Part B. To obtain these marks you should provide a graph showing how gas price has changed over time, a graph showing how gas used for contract transactions has changed over time and identify if the most popular contracts use more or less than the average gas_used. (20%/40%)

    Data Overhead: The blocks table contains a lot of information that may not strictly be necessary for a functioning cryptocurrency e.g. logs_bloom, sha3_uncles, transactions_root, state_root, receipts_root. Analyse how much space would be saved if these columns were removed. Note that all of these values are hex_strings so you can assume that each character after the first two requires four bits (this is not the case in reality but it would be possible to implement it like this) as this will affect your calculations. (20%/40%)
