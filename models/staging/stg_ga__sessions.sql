with source as (
	select * from {{ source('umg_ga', 'ga_sessions_cut') }}
),

renamed as (
    select
        concat(fullVisitorId, cast(visitId as string)) as session_id, -- unique session id
		clientId as client_id, -- unique browser/device id, session agnostic
		fullVisitorId as full_visitor_id, -- unique visitor id, hashed version of client id
		visitNumber as session_number, -- user's first session = 1
		visitId as user_session_id, -- session id, only unique to the user
        timestamp_seconds(visitStartTime) as session_start,
		parse_date("%Y%m%d", date) as date,
		-- =====================================================================
		-- totals: aggregate session values
		totals.bounces as total_session_bounces,
		totals.hits as total_session_hits,
		totals.newVisits as session_is_new_visit, -- new users in session (1=user's first visit, null otherwise)
		totals.pageviews as session_pageviews,
		totals.sessionQualityDim as session_quality, -- closeness to transacting (1-100)
		totals.timeOnSite as total_session_time,
		totals.totalTransactionRevenue as session_revenue,
		totals.transactions as total_session_transactions,
		totals.visits as session_interacted, -- 1 if session had interaction events, null otherwise
		-- =====================================================================

		-- =====================================================================
		-- trafficSource: info about session origins
		trafficSource.adContent as ad_content,
		trafficSource.campaign as ad_campaign,
		trafficSource.isTrueDirect as is_direct, -- true if direct or 2 succesive sessions with same details, otherwise null
		trafficSource.keyword as keyword,
		trafficSource.medium as medium,
		trafficSource.referralPath as referral_path,
		trafficSource.source as source,
		-- =====================================================================

		channelGrouping as channel,

		-- =====================================================================
		-- device: user device info
		device.browser as browser,
		device.browserSize as browser_size,
		device.browserVersion as browser_version,
		device.mobileDeviceInfo as mobile_device_info,
		device.mobileDeviceMarketingName as mobile_device_marketing_name,
		device.mobileDeviceModel as mobile_device_model,
		device.mobileInputSelector as mobile_input,
		device.operatingSystem as os,
		device.operatingSystemVersion as os_version,
		device.mobileDeviceBranding as mobile_device_branding,
		device.language as language,
		device.screenColors as screen_colors,
		device.screenResolution as screen_resolution,
		-- =====================================================================

		-- =====================================================================
		-- geographical data
		geoNetwork.continent as continent,
		geoNetwork.subContinent as sub_continent,
		geoNetwork.country as country,
		geoNetwork.region as region,
		geoNetwork.metro as metro,
		geoNetwork.city as city,
		geoNetwork.cityId as city_id,
		geoNetwork.latitude as latitude,
		geoNetwork.longitude as longitude,
		-- =====================================================================

		-- =====================================================================
		-- hits
		hits.hitNumber as hit_number,
		hits.time as hit_time,
		hits.hour as hit_hour,
		parse_timestamp('%Y%m%d %H', concat(date, ' ', hits.hour)) as utc_hour,
		hits.minute as hit_minute,
		hits.eventInfo.eventCategory as event_category,
		hits.eventInfo.eventAction as event_action,

		case when hits.eCommerceAction.action_type = '1' then 'click through' -- 'Click through of product lists'
			 when hits.eCommerceAction.action_type = '2' then 'view' -- 'Product detail views'
			 when hits.eCommerceAction.action_type = '3' then 'add' -- 'Add product(s) to cart'
			 when hits.eCommerceAction.action_type = '4' then 'remove' -- 'Remove product(s) from cart'
			 when hits.eCommerceAction.action_type = '5' then 'check out' -- 'Check out'
			 when hits.eCommerceAction.action_type = '6' then 'purchase' -- 'Completed purchase'
			 when hits.eCommerceAction.action_type = '7' then 'refund' -- 'Refund of purchase'
			 when hits.eCommerceAction.action_type = '8' then 'opts' -- 'Checkout options'
			 when hits.eCommerceAction.action_type = '0' then 'unknown' -- 'Unknown'
		end as action,

		-- Product
		-- product.isImpression as product_viewed, -- BOOLEAN 	TRUE if at least one user viewed this product (i.e., at least one impression) when it appeared in the product list.
		-- product.isClick as product_clicked, -- BOOLEAN 	Whether users clicked this product when it appeared in the product list.
		product.productListName as product_list_name, -- STRING 	Name of the list in which the product is shown, or in which a click occurred. For example, "Home Page Promotion", "Also Viewed", "Recommended For You", "Search Results List", etc.
		product.productListPosition as product_list_position, -- INTEGER 	Position of the product in the list in which it is shown.
		product.localProductPrice as product_local_price, -- INTEGER 	The price of the product in local currency, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		product.localProductRefundAmount as product_local_refund_amount, -- INTEGER 	The amount processed as part of a refund for a product in local currency, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		product.localProductRevenue as product_local_revenue, -- INTEGER 	The revenue of the product in local currency, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		product.productBrand as product_brand, -- STRING 	The brand associated with the product.
		product.productPrice as product_price, -- INTEGER 	The price of the product, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		product.productQuantity as product_quantity, -- INTEGER 	The quantity of the product purchased.
		product.productRefundAmount as product_refund_amount, -- INTEGER 	The amount processed as part of a refund for a product, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		product.productRevenue as product_revenue, -- INTEGER 	The revenue of the product, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		product.productSKU as sku, -- STRING 	Product SKU.
		product.productVariant as product_variant, -- STRING 	Product Variant.
		product.v2ProductCategory as product_category, -- STRING 	Product Category.
		product.v2ProductName as product_name, -- STRING 	Product Name.

		-- Transaction
		hits.transaction.transactionId as transaction_id, -- STRING 	The transaction ID of the ecommerce transaction.
		hits.transaction.transactionRevenue as transaction_revenue, -- INTEGER 	Total transaction revenue, expressed as the value passed to Analytics multiplied by 10^6. (e.g., 2.40 would be given as 2400000).
		hits.transaction.transactionTax as transaction_tax, -- INTEGER 	Total transaction tax, expressed as the value passed to Analytics multiplied by 10^6. (e.g., 2.40 would be given as 2400000).
		hits.transaction.transactionShipping as transaction_shipping, -- INTEGER 	Total transaction shipping cost, expressed as the value passed to Analytics multiplied by 10^6. (e.g., 2.40 would be given as 2400000).
		hits.transaction.affiliation as affiliation, -- STRING 	The affiliate information passed to the ecommerce tracking code.
		hits.transaction.currencyCode as currency_code, -- STRING 	The local currency code for the transaction.
		hits.transaction.localTransactionRevenue as transaction_local_revenue, -- INTEGER 	Total transaction revenue in local currency, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		hits.transaction.localTransactionTax as transaction_local_tax, -- INTEGER 	Total transaction tax in local currency, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		hits.transaction.localTransactionShipping as transaction_local_shipping, -- INTEGER 	Total transaction shipping cost in local currency, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		-- =====================================================================

    from source, unnest(source.hits) as hits, unnest(hits.product) as product -- no results on 2022/06/06 data, all hits.product = []
)

select * from renamed
