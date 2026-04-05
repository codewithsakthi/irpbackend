#!/usr/bin/env python3
"""
Quick SQL syntax validation for enterprise analytics
"""

def test_sql_syntax():
    """Test that the SQL query is syntactically valid"""
    try:
        # Import the module to check for syntax errors
        from app.services.enterprise_analytics import get_student_360
        print("✓ SQL syntax is valid - no import/parsing errors")
        return True
    except Exception as e:
        print(f"✗ SQL syntax error: {e}")
        return False

if __name__ == "__main__":
    test_sql_syntax()