Naming conventions:

- Fact tables are prefixed with `Fact_`
- Dimension tables are prefixed with `Dim_`
- Bridge tables are prefixed with `Bridge_`
- Fact tables are in plural form
- Dimension tables are in singular form
- Bridge tables are in plural form
- Columns are in CamelCase
- Primary keys are named `Key`
- Foreign keys are prefixed with the referenced table name
- Foreign keys are in singular form
- Foreign keys are suffixed with `Key`

Fixtures:

- `fixtures/cluedin-0.parquet` empty CluedIn instance
- `fixtures/cluedin-1.parquet` Salesforce Organizations and Contacts
- `fixtures/cluedin-2.parquet` CRM Organizations and Contacts
- `fixtures/cluedin-3.parquet` mapped `/Person` name, last name, and email to core vocabularies
- `fixtures/cluedin-4.parquet` tag invalid emails
- `fixtures/cluedin-5.parquet` merge contacts
- `fixtures/cluedin-6.parquet` fix invalid emails

```mermaid
stateDiagram-v2
    [*] --> Entities
    Entities --> Fact_Entities
    Fact_Entities --> Dim_Date
    Fact_Entities --> Dim_EntityType
    Fact_Entities --> Dim_Property
    Fact_Entities --> Dim_Origins
    Fact_Entities --> Dim_Tag
    Fact_Entities --> Bridge_Entities_Tags
    Dim_Date --> Fact_DataQuality
    Dim_EntityType --> Fact_DataQuality
    Dim_Property --> Fact_DataQuality
    Dim_Origins --> Fact_DataQuality
    Dim_Tag --> Fact_DataQuality
```


```mermaid
erDiagram
    Fact_Entities {
        string id PK
        string name
        string entityType
        string discoveryDate
        string createdDate
        string modifiedDate
        string codes
        string tags
        string organization_name
        string user_email
        string user_firstName
        string user_lastName
    }
    Fact_Entities }o--|| Entities_Tags : id
    Fact_Entities }o--|| Dim_Date : discoveryDate
    Fact_Entities }o--|| Dim_Date : createdDate
    Fact_Entities }o--|| Dim_Date : modifiedDate
    
    Entities_Tags {
        string Key PK
        string EntityId
        string Tag_Key
    }
    Entities_Tags }o--|| Dim_Tag : Tag_Key


    Fact_DataQuality {
        string Key PK
        string DateKey
        string EntityTypeKey
        string OriginsKey
        string TagKey
        string MetricKey
        decimal MetricValue
    }
    Fact_DataQuality }o--|| Dim_Metrics : Metric
    Fact_DataQuality }o--|| Dim_Date : Date
    Fact_DataQuality }o--|| Dim_EntityType : EntityType
    Fact_DataQuality }o--|| Dim_Property : Property
    Fact_DataQuality }o--|| Dim_Origins : Origins
    Fact_DataQuality }o--|| Dim_Tag : Tag

    Dim_Metrics {
        string Metric PK
        string MetricName
        string MetricType
        string MetricDescription
    }

    Dim_Date {
        string Date PK
        string Day
        string Month
        string Quarter
        string Year
    }

    Dim_EntityType {
        string EntityType PK
        string EntityTypeName
    }

    Dim_Property {
        string Property PK
        string PropertyName
    }

    Dim_Origins {
        string Origins PK
    }

    Dim_Tag {
        string Tag PK
        string TagName
    }

```