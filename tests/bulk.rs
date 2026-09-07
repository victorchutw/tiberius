use futures_util::io::{AsyncRead, AsyncWrite};
use names::{Generator, Name};
use once_cell::sync::Lazy;
use std::cell::RefCell;
use std::env;
use std::sync::Once;
use tiberius::{numeric::Numeric, ColumnData, IntoSql, Result, TokenRow};

#[cfg(all(feature = "tds73", feature = "chrono"))]
use chrono::DateTime;
#[cfg(all(feature = "tds73", feature = "chrono"))]
use chrono::NaiveDateTime;

use runtimes_macro::test_on_runtimes;

// This is used in the testing macro :)
#[allow(dead_code)]
static LOGGER_SETUP: Once = Once::new();

static CONN_STR: Lazy<String> = Lazy::new(|| {
    env::var("TIBERIUS_TEST_CONNECTION_STRING").unwrap_or_else(|_| {
        "server=tcp:localhost,1433;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
    })
});

thread_local! {
    static NAMES: RefCell<Option<Generator<'static>>> =
    RefCell::new(None);
}

async fn random_table() -> String {
    NAMES.with(|maybe_generator| {
        maybe_generator
            .borrow_mut()
            .get_or_insert_with(|| Generator::with_naming(Name::Plain))
            .next()
            .unwrap()
            .replace('-', "")
    })
}

macro_rules! test_bulk_type {
    ($name:ident($sql_type:literal, $total_generated:expr, $generator:expr)) => {
        paste::item! {
            #[test_on_runtimes]
            async fn [< bulk_load_optional_ $name >]<S>(mut conn: tiberius::Client<S>) -> Result<()>
            where
                S: AsyncRead + AsyncWrite + Unpin + Send,
            {
                let table = format!("##{}", random_table().await);

                conn.execute(
                    &format!(
                        "CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content {} NULL)",
                        table,
                        $sql_type,
                    ),
                    &[],
                )
                    .await?;

                let mut req = conn.bulk_insert(&table).await?;

                for i in $generator {
                    let mut row = TokenRow::new();
                    row.push(i.into_sql());
                    req.send(row).await?;
                }

                let res = req.finalize().await?;

                assert_eq!($total_generated, res.total());

                Ok(())
            }

            #[test_on_runtimes]
            async fn [< bulk_load_required_ $name >]<S>(mut conn: tiberius::Client<S>) -> Result<()>
            where
                S: AsyncRead + AsyncWrite + Unpin + Send,
            {
                let table = format!("##{}", random_table().await);

                conn.execute(
                    &format!(
                        "CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content {} NOT NULL)",
                        table,
                        $sql_type
                    ),
                    &[],
                )
                    .await?;

                let mut req = conn.bulk_insert(&table).await?;

                for i in $generator {
                    let mut row = TokenRow::new();
                    row.push(i.into_sql());
                    req.send(row).await?;
                }

                let res = req.finalize().await?;

                assert_eq!($total_generated, res.total());

                Ok(())
            }
        }
    };
}

test_bulk_type!(tinyint("TINYINT", 256, 0..=255u8));
test_bulk_type!(smallint("SMALLINT", 2000, 0..2000i16));
test_bulk_type!(int("INT", 2000, 0..2000i32));
test_bulk_type!(bigint("BIGINT", 2000, 0..2000i64));

test_bulk_type!(empty_varchar(
    "VARCHAR(MAX)",
    100,
    vec![""; 100].into_iter()
));
test_bulk_type!(empty_nvarchar(
    "NVARCHAR(MAX)",
    100,
    vec![""; 100].into_iter()
));
test_bulk_type!(empty_varbinary(
    "VARBINARY(MAX)",
    100,
    vec![b""; 100].into_iter()
));

test_bulk_type!(real(
    "REAL",
    1000,
    vec![std::f32::consts::PI; 1000].into_iter()
));

test_bulk_type!(float(
    "FLOAT",
    1000,
    vec![std::f64::consts::PI; 1000].into_iter()
));

test_bulk_type!(varchar_limited(
    "VARCHAR(255)",
    1000,
    vec!["aaaaaaaaaaaaaaaaaaaaaaa"; 1000].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2(
    "DATETIME2",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_naive("DATETIME2", 100, {
    #[allow(deprecated)]
    let dt = NaiveDateTime::from_timestamp_opt(1658524194, 123456789).unwrap();

    vec![dt; 100].into_iter()
}));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_0(
    "DATETIME2(0)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_1(
    "DATETIME2(1)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_2(
    "DATETIME2(2)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_3(
    "DATETIME2(3)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_4(
    "DATETIME2(4)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_5(
    "DATETIME2(5)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_6(
    "DATETIME2(6)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[cfg(all(feature = "tds73", feature = "chrono"))]
test_bulk_type!(datetime2_7(
    "DATETIME2(7)",
    100,
    vec![DateTime::from_timestamp(1658524194, 123456789); 100].into_iter()
));

#[test_on_runtimes]
async fn bulk_load_decimal_38_38_round_trips_values<S>(mut conn: tiberius::Client<S>) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let table = format!("##{}", random_table().await);

    conn.execute(
        &format!(
            "CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, content DECIMAL(38,38) NULL)",
            table,
        ),
        &[],
    )
    .await?;

    let max_magnitude = 10i128.pow(38) - 1;
    let values = [
        Some(-1),
        Some(0),
        Some(1),
        Some(max_magnitude),
        Some(-max_magnitude),
        None,
    ];
    let expected_text = [
        Some("-0.00000000000000000000000000000000000001"),
        Some("0.00000000000000000000000000000000000000"),
        Some("0.00000000000000000000000000000000000001"),
        Some("0.99999999999999999999999999999999999999"),
        Some("-0.99999999999999999999999999999999999999"),
        None,
    ];

    let mut req = conn.bulk_insert(&table).await?;

    for value in values {
        let mut row = TokenRow::new();
        row.push(match value {
            Some(value) => Numeric::new_with_scale(value, 38).into_sql(),
            None => ColumnData::Numeric(None),
        });
        req.send(row).await?;
    }

    let res = req.finalize().await?;
    assert_eq!(values.len() as u64, res.total());

    let rows = conn
        .query(
            &format!(
                "SELECT content, CONVERT(VARCHAR(60), content) FROM {} ORDER BY id",
                table
            ),
            &[],
        )
        .await?
        .into_first_result()
        .await?;

    assert_eq!(rows.len(), values.len());

    for ((row, value), text) in rows.iter().zip(values).zip(expected_text) {
        let stored: Option<Numeric> = row.get(0);
        assert_eq!(stored.map(|n| n.value()), value);
        assert_eq!(stored.map(|n| n.scale()), value.map(|_| 38));
        assert_eq!(row.get::<&str, _>(1), text);
    }

    Ok(())
}

#[test_on_runtimes]
async fn bulk_load_decimal_fraction_only_length_buckets<S>(
    mut conn: tiberius::Client<S>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    // Fraction-only values on the wire-length bucket edges (9, 19, and 28
    // fractional digits) reach the server in the shorter length form the
    // corrected precision selects, and read back unchanged.
    let table = format!("##{}", random_table().await);

    conn.execute(
        &format!(
            "CREATE TABLE {} (id INT IDENTITY PRIMARY KEY, \
             s9 DECIMAL(38,9) NOT NULL, s19 DECIMAL(38,19) NOT NULL, s28 DECIMAL(38,28) NOT NULL)",
            table,
        ),
        &[],
    )
    .await?;

    let scales = [9u8, 19, 28];
    let rows_to_send: Vec<[i128; 3]> = vec![
        [10i128.pow(9) - 1, 10i128.pow(19) - 1, 10i128.pow(28) - 1],
        [
            -(10i128.pow(9) - 1),
            -(10i128.pow(19) - 1),
            -(10i128.pow(28) - 1),
        ],
        [1, 1, 1],
        [0, 0, 0],
    ];

    let mut req = conn.bulk_insert(&table).await?;

    for values in &rows_to_send {
        let mut row = TokenRow::new();
        for (value, scale) in values.iter().zip(scales) {
            row.push(Numeric::new_with_scale(*value, scale).into_sql());
        }
        req.send(row).await?;
    }

    let res = req.finalize().await?;
    assert_eq!(rows_to_send.len() as u64, res.total());

    let rows = conn
        .query(
            &format!("SELECT s9, s19, s28 FROM {} ORDER BY id", table),
            &[],
        )
        .await?
        .into_first_result()
        .await?;

    assert_eq!(rows.len(), rows_to_send.len());

    for (row, values) in rows.iter().zip(&rows_to_send) {
        for (index, (value, scale)) in values.iter().zip(scales).enumerate() {
            let stored: Numeric = row.get(index).unwrap();
            assert_eq!(stored.value(), *value, "column {index}");
            assert_eq!(stored.scale(), scale, "column {index}");
        }
    }

    Ok(())
}

#[test_on_runtimes]
async fn bulk_selected_columns_defaults_nulls_identity_order_and_rollback<S>(
    mut conn: tiberius::Client<S>,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let table = format!("##{}", random_table().await);
    conn.execute(
        &format!(
            "CREATE TABLE {table} (id INT IDENTITY PRIMARY KEY, [left] INT NULL, \
         omitted INT NOT NULL DEFAULT 42, money_value MONEY NULL, \
         small_money SMALLMONEY NULL, variant SQL_VARIANT NULL, \
         [right]] value] INT NULL, explicit_null INT NULL, \
         selected_default INT NULL DEFAULT 99, calculated AS ([left] + 1))"
        ),
        &[],
    )
    .await?;

    // Reject the entire selection instead of silently dropping a column, and
    // leave the connection usable after every validation error.
    for columns in [
        &[][..],
        &["left", "left"],
        &["missing"],
        &["id"],
        &["calculated"],
    ] {
        assert!(conn
            .bulk_insert_with_columns(&table, columns)
            .await
            .is_err());
        let row = conn
            .query("SELECT 1", &[])
            .await?
            .into_row()
            .await?
            .unwrap();
        assert_eq!(row.get::<i32, _>(0), Some(1));
    }

    for rollback in [true, false] {
        conn.simple_query("BEGIN TRAN")
            .await?
            .into_results()
            .await?;
        let mut req = conn
            .bulk_insert_with_columns(
                &table,
                &["right] value", "left", "explicit_null", "selected_default"],
            )
            .await?;
        let mut row = TokenRow::new();
        row.push(22i32.into_sql());
        row.push(11i32.into_sql());
        row.push(ColumnData::I32(None));
        row.push(ColumnData::I32(None));
        req.send(row).await?;
        assert_eq!(req.finalize().await?.total(), 1);
        let row = conn
            .query(
                &format!(
                    "SELECT id, [left], [right]] value], omitted, money_value, \
             small_money, explicit_null, selected_default FROM {table}"
                ),
                &[],
            )
            .await?
            .into_row()
            .await?
            .unwrap();
        assert!(row.get::<i32, _>(0).unwrap() > 0);
        assert_eq!(row.get::<i32, _>(1), Some(11));
        assert_eq!(row.get::<i32, _>(2), Some(22));
        assert_eq!(row.get::<i32, _>(3), Some(42));
        assert_eq!(row.get::<f64, _>(4), None);
        assert_eq!(row.get::<f64, _>(5), None);
        // SQL_VARIANT is unsupported by the decoder for non-null values;
        // prove its omitted NULL using server-side predicates below.
        assert_eq!(row.get::<i32, _>(6), None);
        assert_eq!(row.get::<i32, _>(7), Some(99));
        conn.simple_query(if rollback {
            "ROLLBACK TRAN"
        } else {
            "COMMIT TRAN"
        })
        .await?
        .into_results()
        .await?;
        let row = conn
            .query(
                &format!("SELECT COUNT(*) FROM {table} WHERE variant IS NULL"),
                &[],
            )
            .await?
            .into_row()
            .await?
            .unwrap();
        assert_eq!(row.get::<i32, _>(0), Some(if rollback { 0 } else { 1 }));
    }
    Ok(())
}
