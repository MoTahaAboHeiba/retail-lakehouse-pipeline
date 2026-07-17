{#
  Metadata-driven business layer (One Big Table).
  Config drives both column selection and join generation via ref().

  IMPORTANT: list order below encodes join dependency order.
  orders_tech must stay first (it's the join anchor).
  order_items_tech must stay before products_tech, because products_tech
  joins on oi.product_id, not o.product_id.
  Do not reorder this list without checking each entry's join_condition
  against what alias it depends on.
#}

{% set configs = [
    {
        "ref_name": "orders_tech",
        "alias": "o",
        "join_condition": none,
        "columns": [
            {"col": "order_id"},
            {"col": "store_id"},
            {"col": "customer_id"},
            {"col": "order_timestamp"},
            {"col": "payment_method"},
            {"col": "order_status"},
            {"col": "total_amount"},
            {"col": "created_timestamp", "as": "order_created_timestamp"},
            {"col": "updated_timestamp", "as": "order_updated_timestamp"},
            {"col": "is_active", "as": "order_is_active"},
            {"col": "processed_at", "as": "order_processed_at"}
        ]
    },
    {
        "ref_name": "customers_tech",
        "alias": "c",
        "join_condition": "o.customer_id = c.customer_id",
        "columns": [
            {"col": "first_name", "as": "customer_first_name"},
            {"col": "last_name", "as": "customer_last_name"},
            {"col": "email", "as": "customer_email"},
            {"col": "phone", "as": "customer_phone"},
            {"col": "city", "as": "customer_city"},
            {"col": "province", "as": "customer_province"},
            {"col": "country", "as": "customer_country"},
            {"col": "created_timestamp", "as": "customer_created_timestamp"},
            {"col": "updated_timestamp", "as": "customer_updated_timestamp"},
            {"col": "is_active", "as": "customer_is_active"},
            {"col": "processed_at", "as": "customer_processed_at"}
        ]
    },
    {
        "ref_name": "order_items_tech",
        "alias": "oi",
        "join_condition": "o.order_id = oi.order_id",
        "columns": [
            {"col": "order_item_id"},
            {"col": "order_id", "as": "order_item_order_id"},
            {"col": "product_id"},
            {"col": "quantity"},
            {"col": "unit_price"},
            {"col": "line_amount"},
            {"col": "created_timestamp", "as": "order_item_created_timestamp"},
            {"col": "updated_timestamp", "as": "order_item_updated_timestamp"},
            {"col": "is_active", "as": "order_item_is_active"},
            {"col": "processed_at", "as": "order_item_processed_at"}
        ]
    },
    {
        "ref_name": "products_tech",
        "alias": "p",
        "join_condition": "oi.product_id = p.product_id",
        "columns": [
            {"col": "product_name"},
            {"col": "category"},
            {"col": "brand"},
            {"col": "price"},
            {"col": "created_timestamp", "as": "product_created_timestamp"},
            {"col": "updated_timestamp", "as": "product_updated_timestamp"},
            {"col": "is_active", "as": "product_is_active"},
            {"col": "processed_at", "as": "product_processed_at"}
        ]
    },
    {
        "ref_name": "stores_tech",
        "alias": "s",
        "join_condition": "o.store_id = s.store_id",
        "columns": [
            {"col": "store_id", "as": "store_store_id"},
            {"col": "store_name"},
            {"col": "city", "as": "store_city"},
            {"col": "province", "as": "store_province"},
            {"col": "country", "as": "store_country"},
            {"col": "created_timestamp", "as": "store_created_timestamp"},
            {"col": "updated_timestamp", "as": "store_updated_timestamp"},
            {"col": "is_active", "as": "store_is_active"},
            {"col": "processed_at", "as": "store_processed_at"}
        ]
    }
] %}

SELECT
    {% for config in configs %}
        {% for c in config['columns'] %}
            {{ config['alias'] }}.{{ c['col'] }} AS {{ c.get('as', c['col']) }}{% if not loop.last %},{% endif %}
        {% endfor %}
        {%- if not loop.last %},{% endif %}
    {% endfor %}
    , CONCAT(c.first_name, ' ', c.last_name) AS customer_name
    , current_timestamp() AS obt_b_processed_at

FROM
    {% for config in configs %}
        {% if loop.first %}
            {{ ref(config['ref_name']) }} AS {{ config['alias'] }}
        {% else %}
            LEFT JOIN {{ ref(config['ref_name']) }} AS {{ config['alias'] }}
                ON {{ config['join_condition'] }}
        {% endif %}
    {% endfor %}
