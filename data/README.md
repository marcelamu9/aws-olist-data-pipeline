# Fuente de datos

El proyecto utiliza el **Brazilian E-Commerce Public Dataset by Olist**, disponible en Kaggle:

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

El dataset contiene información histórica de una plataforma de comercio electrónico y está compuesto por varios archivos CSV relacionados entre sí.

## Archivos disponibles

- `olist_orders_dataset.csv`: estado y fechas asociadas a los pedidos.  
  **99,441 registros.**

- `olist_order_items_dataset.csv`: productos incluidos en cada pedido, vendedor, precio y costo de flete.  
  **112,650 registros.**

- `olist_order_payments_dataset.csv`: métodos de pago, cuotas y valor pagado por pedido.  
  **103,886 registros.**

- `olist_sellers_dataset.csv`: información geográfica de los vendedores.  
  **3,095 registros.**

- `olist_customers_dataset.csv`: identificadores y ubicación de los clientes.  
  **99,441 registros.**

- `olist_products_dataset.csv`: categoría y características físicas de los productos.  
  **32,951 registros.**

- `olist_order_reviews_dataset.csv`: puntuaciones y comentarios asociados a la experiencia del cliente.  
  **99,224 registros.**

- `product_category_name_translation.csv`: traducción de las categorías de productos del portugués al inglés.  
  **71 registros.**

- `olist_geolocation_dataset.csv`: información geográfica asociada a códigos postales de Brasil.  
  **1,000,163 registros.**

## Descarga de los datos

Los archivos CSV originales no se almacenan directamente en este repositorio.

Para descargar los datos, ejecute desde la raíz del proyecto:

```bash
python data/get_data.py
```

## Diagrama Entidad – Relación
Se creó un diagrama entidad-relación para visualizar la estructura general del dataset, identificar las llaves lógicas y comprender las relaciones entre pedidos, clientes, productos, vendedores, pagos y reseñas.

![Entidad-Relacion](..\docs\screenshots\DiagramaOlist.png)

