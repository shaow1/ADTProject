//Benchmarks
//1. top 20 products bought together
db.invoices.aggregate([
  // 1. Use index (must be FIRST)
  {
    $match: {
      invoice: { $not: /^C/ },
      invoice_date: {
        $gt: ISODate("2010-01-01T00:00:00Z")
      }
    }
  },

  // 2. Unwind items
  { $unwind: "$items" },

  // 3. Collect list of products per invoice
  {
    $group: {
      _id: "$invoice",
      products: { $push: "$items.stock_code" }
    }
  },

  // 4. Generate all unique pairs inside invoice
  {
    $project: {
      pairs: {
        $function: {
          body: function (products) {
            const out = [];
            for (
              let i = 0;
              i < products.length;
              i++
            ) {
              for (
                let j = i + 1;
                j < products.length;
                j++
              ) {
                out.push([
                  products[i],
                  products[j]
                ]);
              }
            }
            return out;
          },
          args: ["$products"],
          lang: "js"
        }
      }
    }
  },

  // 5. Expand pairs
  { $unwind: "$pairs" },

  // 6. Count frequency of each pair
  {
    $group: {
      _id: {
        p1: { $arrayElemAt: ["$pairs", 0] },
        p2: { $arrayElemAt: ["$pairs", 1] }
      },
      frequency: { $sum: 1 }
    }
  },

  // 7. Apply HAVING threshold
  {
    $match: { frequency: { $gte: 15 } }
  },

  // 8. Sort
  { $sort: { frequency: -1 } },

  // 9. Limit
  { $limit: 20 }
]);

//2. Top-10 Most Bought Products (by total quantity sold)
db.invoices.aggregate([
  // 1. Expand items array
  { $unwind: "$items" },

  // 2. Group by product code + description
  {
    $group: {
      _id: {
        stock_code: "$items.stock_code",
        description: "$items.description"
      },
      total_quantity: { $sum: "$items.quantity" }
    }
  },

  // 3. Sort descending by quantity
  { $sort: { total_quantity: -1 } },

  // 4. Limit top 10
  { $limit: 10 },

  // 5. Make a clean output
  {
    $project: {
      _id: 0,
      stock_code: "$_id.stock_code",
      description: "$_id.description",
      total_quantity: 1
    }
  }
]);

//3. top 50 customers who bought product 85123A
db.invoices.aggregate([
  // 1. Ignore cancelled invoices (invoice starts with 'C')
  { $match: { invoice: { $not: /^C/ } } },

  // 2. Expand items so we can filter product-level details
  { $unwind: "$items" },

  // 3. Filter specific product
  { $match: { "items.stock_code": "85123A" } },

  // 4. Group by customer to compute stats
  {
    $group: {
      _id: {
        customer_id: "$customer_id",
        country: "$country"
      },
      times_bought_this_product: { $addToSet: "$invoice" }, // distinct invoice count
      total_quantity: { $sum: "$items.quantity" },
      last_purchase_date: { $max: "$invoice_date" },
      total_spent: { $sum: { $multiply: ["$items.quantity", "$items.price"] } }
    }
  },

  // 5. Compute final fields
  {
    $project: {
      _id: 0,
      customer_id: "$_id.customer_id",
      country: "$_id.country",
      times_bought_this_product: { $size: "$times_bought_this_product" },
      total_quantity: 1,
      last_purchase_date: 1,
      total_spent: { $round: ["$total_spent", 2] }
      // You can add total_orders if needed
    }
  },

  // 6. Sort like MySQL
  { $sort: { times_bought_this_product: -1 } },

  // 7. Limit 50
  { $limit: 50 }
]);

//4. Top5 best selling product by country
db.invoices.aggregate([
  // 1. Filter invalid/cancelled invoices
  {
    $match: {
      invoice: { $not: /^C/ },
      country: { $ne: null }
    }
  },

  // 2. Unwind items so each purchase is its own row
  { $unwind: "$items" },

  // 3. Group by country + product
  {
    $group: {
      _id: {
        country: "$country",
        stock_code: "$items.stock_code"
      },
      description: { $max: "$items.description" },
      total_quantity: { $sum: "$items.quantity" },
      total_revenue: {
        $sum: { $multiply: ["$items.quantity", "$items.price"] }
      }
    }
  },

  // 4. Reshape fields
  {
    $project: {
      _id: 0,
      country: "$_id.country",
      stock_code: "$_id.stock_code",
      description: 1,
      total_quantity: 1,
      total_revenue: { $round: ["$total_revenue", 2] }
    }
  },

  // 5. Partition by country and rank each product inside its country  
  //    Equivalent of ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_quantity DESC)
  {
    $setWindowFields: {
      partitionBy: "$country",
      sortBy: { total_quantity: -1 },
      output: {
        product_rank: { $documentNumber: {} }
      }
    }
  },

  // 6. Pick top 5 per country
  { $match: { product_rank: { $lte: 5 } } },

  // 7. Sort final output
  { $sort: { country: 1, product_rank: 1 } },

  { $limit: 5 }
]);


//5. Recent Purchases by Similar Customers - Recommend TOP-15 new products
db.invoices.aggregate([
  // 1) Get ALL products purchased by target customer
  { $match: { customer_id: "13085.0", invoice: { $not: /^C/ } } },
  { $unwind: "$items" },
  {
    $group: {
      _id: null,
      target_products: { $addToSet: "$items.stock_code" }
    }
  },

  // 2) Save target_products so we can reuse them
  { $set: { target_products: "$target_products" } },

  // 3) Lookup similar-customer purchases
  {
    $lookup: {
      from: "invoices",
      let: { tps: "$target_products" },
      pipeline: [
        { $match: { invoice: { $not: /^C/ } } },
        { $unwind: "$items" },

        // similar customers = bought ANY product that target customer bought
        {
          $match: {
            $expr: { $in: ["$items.stock_code", "$$tps"] }
          }
        },

        // collect distinct similar customer IDs
        {
          $group: {
            _id: "$customer_id"
          }
        }
      ],
      as: "similar_customers_docs"
    }
  },

  // 4) Extract IDs as array
  {
    $set: {
      similar_customers: {
        $map: {
          input: "$similar_customers_docs",
          as: "x",
          in: "$$x._id"
        }
      }
    }
  },

  // 5) Second lookup: fetch ALL purchases of similar customers
  {
    $lookup: {
      from: "invoices",
      let: {
        sim: "$similar_customers",
        targetProds: "$target_products"
      },
      pipeline: [
        { $match: { invoice: { $not: /^C/ } } },
        { $unwind: "$items" },

        // restrict to similar customers
        {
          $match: {
            $expr: { $in: ["$customer_id", "$$sim"] }
          }
        },

        // remove products target already bought
        {
          $match: {
            $expr: { $not: { $in: ["$items.stock_code", "$$targetProds"] } }
          }
        },

        // group by product
        {
          $group: {
            _id: "$items.stock_code",
            similar_customers: { $addToSet: "$customer_id" },
            total_quantity: { $sum: "$items.quantity" },
            avg_price: { $avg: "$items.price" }
          }
        },

        {
          $project: {
            _id: 0,
            stock_code: "$_id",
            recommended_by_customers: { $size: "$similar_customers" },
            total_quantity: 1,
            avg_price: { $round: ["$avg_price", 2] }
          }
        },

        { $sort: { recommended_by_customers: -1, total_quantity: -1 } },
        { $limit: 15 }
      ],
      as: "recommendations"
    }
  },

  // 6) final output
  { $project: { _id: 0, recommendations: 1 } }
]);

//6. co purchase network customer with similar tastes
db.invoices.aggregate([
    // 1. Filter invoices after 2010-01-01
    {
        $match: {
            invoice_date: { $gt: ISODate("2010-01-01T00:00:00Z") }
        }
    },

    // 2. Deconstruct items to process individual products
    { $unwind: "$items" },

    // 3. Deduplicate: Ensure we only count a product once per customer
    //    (e.g., if they bought the same item 5 times, it's still just 1 "shared interest")
    {
        $group: {
            _id: {
                code: "$items.stock_code",
                cust: "$customer_id"
            }
        }
    },

    // 4. Group by Product (Stock Code) to get an array of ALL buyers for that item
    {
        $group: {
            _id: "$_id.code",
            buyers: { $push: "$_id.cust" }
        }
    },

    // 5. Optimization: Remove products bought by only 0 or 1 person (no pairs possible)
    {
        $match: {
            $expr: { $gte: [{ $size: "$buyers" }, 2] }
        }
    },

    // 6. Generate Combinations (Cartesian Product)
    //    Duplicate the buyers array so we can unwind them against each other
    {
        $project: {
            buyer_A: "$buyers",
            buyer_B: "$buyers"
        }
    },
    { $unwind: "$buyer_A" },
    { $unwind: "$buyer_B" },

    // 7. Filter logic to create unique pairs
    //    1. No self-matches (UserA == UserA)
    //    2. No duplicate pairs (UserA, UserB) and (UserB, UserA) are counted as one
    {
        $match: {
            $expr: { $lt: ["$buyer_A", "$buyer_B"] }
        }
    },

    // 8. Group by the PAIR and count the number of products they passed through together
    {
        $group: {
            _id: {
                customer_1: "$buyer_A",
                customer_2: "$buyer_B"
            },
            shared_products_count: { $sum: 1 }
        }
    },

    // 9. Filter for at least 5 identical products
    {
        $match: {
            shared_products_count: { $gte: 5 }
        }
    },

    // 10. Sort descending (most similar first) and take top 25
    { $sort: { shared_products_count: -1 } },
    { $limit: 25 }

], { allowDiskUse: true })


//7. Customer who bouthgt 85123A would most likely to buy 
db.invoices.aggregate([
    // 1. Find the "Trigger" Invoices (Purchase of 85123A)
    {
        $match: {
            "items.stock_code": "85123A"
        }
    },

    // 2. Join with the same collection to find "Future" Invoices
    {
        $lookup: {
            from: "invoices",
            let: {
                trigger_customer: "$customer_id",
                trigger_date: "$invoice_date"
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                // Must be the same customer
                                { $eq: ["$customer_id", "$$trigger_customer"] },
                                
                                // Must be AFTER the trigger purchase
                                { $gt: ["$invoice_date", "$$trigger_date"] },
                                
                                // Must be WITHIN 6 months (approx 180 days)
                                // using $dateAdd (avail in Mongo 5.0+)
                                { 
                                    $lt: [
                                        "$invoice_date", 
                                        { 
                                            $dateAdd: { 
                                                startDate: "$$trigger_date", 
                                                unit: "month", 
                                                amount: 6 
                                            } 
                                        } 
                                    ] 
                                }
                            ]
                        }
                    }
                },
                // Optimization: We only need the items and date from the future invoice
                { $project: { items: 1, invoice_date: 1 } }
            ],
            as: "future_purchases"
        }
    },

    // 3. Filter out customers who didn't buy anything afterwards
    { $match: { "future_purchases": { $ne: [] } } },

    // 4. Unwind the future invoices to process them
    { $unwind: "$future_purchases" },

    // 5. Unwind the ITEMS inside those future invoices
    { $unwind: "$future_purchases.items" },

    // 6. Filter: Exclude '85123A' itself (we want *other* products)
    { 
        $match: { 
            "future_purchases.items.stock_code": { $ne: "85123A" } 
        } 
    },

    // 7. Calculate the time difference (in days) for this specific next-purchase
    {
        $project: {
            next_product_code: "$future_purchases.items.stock_code",
            next_product_desc: "$future_purchases.items.description",
            days_to_buy: {
                $divide: [
                    { $subtract: ["$future_purchases.invoice_date", "$invoice_date"] },
                    1000 * 60 * 60 * 24 // Convert milliseconds to days
                ]
            }
        }
    },

    // 8. Aggregation: Group by the Next Product
    {
        $group: {
            _id: "$next_product_code",
            product_name: { $first: "$next_product_desc" },
            count: { $sum: 1 },
            avg_days_to_purchase: { $avg: "$days_to_buy" }
        }
    },

    // 9. Sort by most frequent next purchase
    { $sort: { count: -1 } },

    // 10. Show top 20
    { $limit: 20 }

], { allowDiskUse: true })


//8. Top 50 most valuable customers
db.invoices.aggregate([
    // 1. Filter out invalid customer IDs (e.g., Guest checkouts or NaNs)
    //    Adjust "nan" or null based on your actual data cleaning
    { 
        $match: { 
            customer_id: { $nin: ["nan", null, ""] } 
        } 
    },

    // 2. Deconstruct the items array to calculate line totals
    { $unwind: "$items" },

    // 3. Group by Customer ID and sum the calculated revenue
    //    (Quantity * Price) automatically handles negative quantities (returns)
    {
        $group: {
            _id: "$customer_id",
            total_spent: { 
                $sum: { $multiply: ["$items.quantity", "$items.price"] } 
            }
        }
    },

    // 4. Sort by total spent in descending order
    { $sort: { total_spent: -1 } },

    // 5. Limit to the top 50
    { $limit: 50 }
], { allowDiskUse: true })
