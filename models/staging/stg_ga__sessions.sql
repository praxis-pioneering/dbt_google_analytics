with source as (
    select * from {{ source('umg_ga', 'ga_sessions_20220606') }}
),

renamed as (
    select 
        concat(fullVisitorId, cast(visitId as string)) as session_id, -- unique session id
		clientId as client_id, -- unique browser/device id, session agnostic
		fullVisitorId as full_visitor_id, -- unique visitor id, hashed version of client id
		userId as user_id, -- known (logged in) user id
		visitNumber as session_number, -- user's first session = 1
		visitId as user_session_id, -- session id, only unique to the user
        timestamp_seconds(visitStartTime) as session_start,
		-- omitted date, equal to date in session_start
		-- =====================================================================
		-- totals: aggregate session values
		totals.bounces as total_session_bounces,
		totals.hits as total_session_hits,
		totals.newVisits as session_is_new_visit, -- new users in session (1=user's first visit, null otherwise)
		totals.pageviews as session_pageviews,
		totals.screenviews as session_screenviews,
		totals.sessionQualityDim as session_quality, -- closeness to transacting (1-100)
		totals.timeOnScreen as session_time_on_screen,
		totals.timeOnSite as total_session_time,
		totals.totalTransactionRevenue as session_revenue,
		totals.transactions as total_session_transactions,
		totals.uniqueScreenViews as session_unique_screenviews,
		totals.visits as session_interacted, -- 1 if session had interaction events, null otherwise
		-- =====================================================================

		-- =====================================================================
		-- trafficSource: info about session origins
		trafficSource.adContent as ad_content,
		trafficSource.campaign as ad_campaign,
		trafficSource.campaignCode as ad_campaign_code,
		trafficSource.isTrueDirect as direct, -- true if direct or 2 succesive sessions with same details, otherwise null
		trafficSource.keyword as source_keyword, 
		trafficSource.medium as source_medium,
		trafficSource.referralPath as source_referral_path,
		trafficSource.source as source_source,

		-- ad info
		trafficSource.adwordsClickInfo.adGroupId as ad_group_id,
		trafficSource.adwordsClickInfo.adNetworkType as ad_network,
		trafficSource.adwordsClickInfo.campaignId as ad_campaign_id,
		trafficSource.adwordsClickInfo.creativeId as ad_id,
		trafficSource.adwordsClickInfo.criteriaId as ad_criteria_id,
		trafficSource.adwordsClickInfo.criteriaParameters as ad_criteria_params,
		trafficSource.adwordsClickInfo.customerId as ad_customer_id,
		trafficSource.adwordsClickInfo.gclId as ad_click_id,
		trafficSource.adwordsClickInfo.isVideoAd as ad_is_video,
		trafficSource.adwordsClickInfo.page as ad_page,
		trafficSource.adwordsClickInfo.slot as ad_slot,
		-- =====================================================================

		socialEngagementType as social_engagement_type, -- "Socially Engaged" or "Not Socially Engaged"
		channelGrouping as channel_grouping,


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
		device.flashVersion as flash_version,
		device.javaEnabled as java_enabled,
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
		hits.isEntrance as hit_is_session_entrance, -- hit was first pageview/screenview of session
		hits.isExit as hit_is_session_exit, -- hit was last pageview/screenview
		hits.isInteraction as hit_is_interaction,


		hits.time as hit_time,
		hits.hour as hit_hour,
		hits.minute as hit_minute,

		hits.referer as hit_referer,
		hits.type as hit_type,
		hits.eventInfo.eventCategory,
		hits.eventInfo.eventAction,
		hits.eventInfo.eventLabel,
		hits.eventInfo.eventValue,

		case when hits.eCommerceAction.action_type = '1' then 'Click through of product lists'
			 when hits.eCommerceAction.action_type = '2' then 'Product detail views'
			 when hits.eCommerceAction.action_type = '3' then 'Add product(s) to cart'
			 when hits.eCommerceAction.action_type = '4' then 'Remove product(s) from cart'
			 when hits.eCommerceAction.action_type = '5' then 'Check out'
			 when hits.eCommerceAction.action_type = '6' then 'Completed purchase'
			 when hits.eCommerceAction.action_type = '7' then 'Refund of purchase'
			 when hits.eCommerceAction.action_type = '8' then 'Checkout options'
			 when hits.eCommerceAction.action_type = '0' then 'Unknown' 
		end as ecommerce_action_type_desc,

		-- Product
		-- product.isImpression as product_viewed, -- BOOLEAN 	TRUE if at least one user viewed this product (i.e., at least one impression) when it appeared in the product list.
		-- product.isClick as product_clicked, -- BOOLEAN 	Whether users clicked this product when it appeared in the product list.
		-- product.productListName as product_list_name, -- STRING 	Name of the list in which the product is shown, or in which a click occurred. For example, "Home Page Promotion", "Also Viewed", "Recommended For You", "Search Results List", etc.
		-- product.productListPosition as product_list_position, -- INTEGER 	Position of the product in the list in which it is shown.
		-- product.localProductPrice as product_local_price, -- INTEGER 	The price of the product in local currency, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		-- product.localProductRefundAmount as product_refund_amount, -- INTEGER 	The amount processed as part of a refund for a product in local currency, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		-- product.localProductRevenue as product_local_revenue, -- INTEGER 	The revenue of the product in local currency, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		-- product.productBrand as product_brand, -- STRING 	The brand associated with the product.
		-- product.productPrice as product_price, -- INTEGER 	The price of the product, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		-- product.productQuantity as product_quantity, -- INTEGER 	The quantity of the product purchased.
		-- product.productRefundAmount as product_refund_amount, -- INTEGER 	The amount processed as part of a refund for a product, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		-- product.productRevenue as product_revenue, -- INTEGER 	The revenue of the product, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		-- product.productSKU as product_sku, -- STRING 	Product SKU.
		-- product.productVariant as product_variant, -- STRING 	Product Variant.
		-- product.v2ProductCategory as product_category, -- STRING 	Product Category.
		-- product.v2ProductName as product_name, -- STRING 	Product Name.

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

		-- Item
		hits.item.transactionId as item_transaction_id, -- STRING 	The transaction ID of the ecommerce transaction.
		hits.item.productName as item_product_name, -- STRING 	The name of the product.
		hits.item.productCategory as item_product_category, -- STRING 	The category of the product.
		hits.item.productSku as item_product_sku, -- STRING 	The SKU or product ID.
		hits.item.itemQuantity as item_quantity, -- INTEGER 	The quantity of the product sold.
		hits.item.itemRevenue as item_revenue, -- INTEGER 	Total revenue from the item, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		hits.item.currencyCode as item_currency_code, -- STRING 	The local currency code for the transaction.
		hits.item.localItemRevenue as item_local_revenue, -- INTEGER 	Total revenue from this item in local currency, expressed as the value passed to Analytics multiplied by 10^6 (e.g., 2.40 would be given as 2400000).
		-- =====================================================================

    from source, unnest(source.hits) as hits --, unnest(hits.product) as product -- no results on 2022/06/06 data, all hits.product = []
)

select * from renamed -- where item_product_sku is not null
