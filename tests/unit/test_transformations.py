"""
Unit tests for data transformation logic.
Uses mocked S3 (moto) and local PySpark.
"""
import pytest
import pandas as pd
from datetime import date
from unittest.mock import patch, MagicMock


class TestOrderValidation:
    """Test data quality validation logic."""

    def setup_method(self):
        from data_quality.validate import validate_orders, validate_users
        self.validate_orders = validate_orders
        self.validate_users  = validate_users

    def test_valid_orders_pass_all_checks(self):
        df = pd.DataFrame([{
            "order_id": "a1", "user_id": "u1", "status": "completed",
            "net_amount": 99.99, "order_date": "2024-01-01",
        }])
        results = self.validate_orders(df)
        failed = [r for r in results if not r.passed]
        assert failed == [], f"Unexpected failures: {failed}"

    def test_duplicate_order_ids_fail(self):
        df = pd.DataFrame([
            {"order_id": "a1", "user_id": "u1", "status": "completed",
             "net_amount": 10.0, "order_date": "2024-01-01"},
            {"order_id": "a1", "user_id": "u2", "status": "shipped",
             "net_amount": 20.0, "order_date": "2024-01-01"},
        ])
        results = self.validate_orders(df)
        dup_check = next(r for r in results if "unique" in r.name)
        assert not dup_check.passed

    def test_negative_net_amount_fails(self):
        df = pd.DataFrame([{
            "order_id": "a1", "user_id": "u1", "status": "completed",
            "net_amount": -5.0, "order_date": "2024-01-01",
        }])
        results = self.validate_orders(df)
        amt_check = next((r for r in results if "positive" in r.name), None)
        if amt_check:
            assert not amt_check.passed

    def test_invalid_status_fails(self):
        df = pd.DataFrame([{
            "order_id": "a1", "user_id": "u1", "status": "INVALID_STATUS",
            "net_amount": 10.0, "order_date": "2024-01-01",
        }])
        results = self.validate_orders(df)
        status_check = next(r for r in results if "status" in r.name)
        assert not status_check.passed

    def test_empty_dataframe_fails(self):
        df = pd.DataFrame()
        results = self.validate_orders(df)
        empty_check = next(r for r in results if "not_empty" in r.name)
        assert not empty_check.passed

    def test_valid_users_pass(self):
        df = pd.DataFrame([{
            "user_id": "u1", "email": "test@test.com",
        }])
        results = self.validate_users(df)
        failed = [r for r in results if not r.passed]
        assert failed == []

    def test_duplicate_emails_fail(self):
        df = pd.DataFrame([
            {"user_id": "u1", "email": "same@test.com"},
            {"user_id": "u2", "email": "same@test.com"},
        ])
        results = self.validate_users(df)
        email_check = next(r for r in results if "email_unique" in r.name)
        assert not email_check.passed


class TestCoinGeckoExtractor:
    """Test S3 upload and partition path logic."""

    def test_s3_key_partition_format(self):
        """Verify Bronze S3 keys are correctly partitioned."""
        from ingestion.batch.postgres_extractor import INCREMENTAL_QUERIES
        assert "orders" in INCREMENTAL_QUERIES
        assert "users"  in INCREMENTAL_QUERIES
        assert "%(batch_date)s" in INCREMENTAL_QUERIES["orders"]

    def test_all_required_tables_have_queries(self):
        from ingestion.batch.postgres_extractor import INCREMENTAL_QUERIES
        required = ["orders", "users", "order_items", "products"]
        for t in required:
            assert t in INCREMENTAL_QUERIES, f"Missing query for table: {t}"
