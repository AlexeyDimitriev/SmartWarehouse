# Smart Warehouse
Сервис для получения событий от Producer через Apache Kafka для хранения в БД Cassandra

## Cassandra nodes

### inventory_by_product_zone

Используется для получения остатка товара в конкретной зоне.  
Partition_key: product_id (эффективный поиск по id товара)  
Clustering_key: zone_id (для возможной поддержки запросов по диапозону зон)  

### inventory_by_product

Используется для получения остатка товара в конкретной зоне.  
Partition_key: product_id (эффективный поиск по id товара)  
Clustering_key: None  

### inventory_by_zone

Используется для получения остатка товара в конкретной зоне.  
Partition_key: zone_id (эффективный поиск по id зоны)  
Clustering_key: product_id (необязательно, но так товары будут упорядочены по id)