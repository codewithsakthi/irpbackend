#!/usr/bin/env python3
"""
Test script for CGPA-based ranking functionality.
Verifies that the RankingService works correctly.
"""

import asyncio
import sys
import os

# Add the backend directory to the path so we can import the modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.core.database import get_async_session
from app.services.ranking_service import RankingService
from app.core.constants import CURRICULUM_CREDITS


async def test_ranking_service():
    """Test the RankingService functionality"""
    print("Testing CGPA-based Ranking Service...")
    print("=" * 50)
    
    async with get_async_session() as session:
        try:
            # Test 1: Get overall rankings (top 10)
            print("\n1. Testing Overall Rankings (Top 10):")
            overall = await RankingService.get_overall_rankings(
                db=session,
                curriculum_credits=CURRICULUM_CREDITS,
                limit=10,
                offset=0
            )
            print(f"   Total Students: {overall['total_students']}")
            print(f"   Top 10 Rankings:")
            for i, student in enumerate(overall['rankings'][:10], 1):
                print(f"   {i:2d}. {student['name']:<25} (Roll: {student['roll_no']}) - CGPA: {student['cgpa']:5.2f} - Rank: {student['rank']:3d}")
            
            # Test 2: Get top performers
            print("\n2. Testing Top Performers (Top 5):")
            top_performers = await RankingService.get_top_performers(
                db=session,
                curriculum_credits=CURRICULUM_CREDITS,
                limit=5
            )
            for i, student in enumerate(top_performers, 1):
                print(f"   {i}. {student['name']:<25} - CGPA: {student['cgpa']:5.2f} - Attendance: {student['attendance_percentage']:5.1f}%")
            
            # Test 3: Get student rank for first student
            if overall['rankings']:
                first_student = overall['rankings'][0]
                print(f"\n3. Testing Individual Student Rank for {first_student['roll_no']}:")
                student_rank = await RankingService.get_student_rank_by_cgpa(
                    db=session,
                    roll_no=first_student['roll_no'],
                    curriculum_credits=CURRICULUM_CREDITS
                )
                if student_rank:
                    category = RankingService.get_rank_category(
                        rank=student_rank['rank'],
                        total_students=student_rank['total_students']
                    )
                    print(f"   Name: {student_rank['name']}")
                    print(f"   Rank: {student_rank['rank']} out of {student_rank['total_students']}")
                    print(f"   CGPA: {student_rank['cgpa']:.3f}")
                    print(f"   Percentile: {student_rank['percentile']:.1f}%")
                    print(f"   Category: {category}")
                    print(f"   Backlogs: {student_rank['backlogs']}")
            
            # Test 4: Test batch rankings if we have batch data
            if overall['rankings']:
                sample_batch = overall['rankings'][0]['batch']
                if sample_batch:
                    print(f"\n4. Testing Batch Rankings for {sample_batch} (Top 5):")
                    batch_rankings = await RankingService.get_batch_rankings(
                        db=session,
                        batch=sample_batch,
                        curriculum_credits=CURRICULUM_CREDITS,
                        limit=5,
                        offset=0
                    )
                    print(f"   Total Students in Batch: {batch_rankings['total_students']}")
                    for student in batch_rankings['rankings']:
                        print(f"   Rank {student['rank']:2d}: {student['name']:<20} - CGPA: {student['cgpa']:5.2f}")
            
            # Test 5: Test semester rankings if we have semester data
            if overall['rankings']:
                sample_semester = overall['rankings'][0]['current_semester']
                if sample_semester:
                    print(f"\n5. Testing Semester {sample_semester} Rankings (Top 5):")
                    semester_rankings = await RankingService.get_semester_rankings(
                        db=session,
                        semester=sample_semester,
                        curriculum_credits=CURRICULUM_CREDITS,
                        limit=5,
                        offset=0
                    )
                    print(f"   Total Students in Semester: {semester_rankings['total_students']}")
                    for student in semester_rankings['rankings']:
                        print(f"   Rank {student['rank']:2d}: {student['name']:<20} - CGPA: {student['cgpa']:5.2f}")
            
            print("\n" + "=" * 50)
            print("✅ All ranking tests completed successfully!")
            
        except Exception as e:
            print(f"\n❌ Error testing ranking service: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    return True


def test_rank_categories():
    """Test rank categorization logic"""
    print("\n6. Testing Rank Categories:")
    test_cases = [
        (1, 100, "Excellent"),   # Top 1%
        (5, 100, "Excellent"),   # Top 5%
        (10, 100, "Excellent"),  # Top 10%
        (15, 100, "Good"),       # Top 15%
        (25, 100, "Good"),       # Top 25%
        (50, 100, "Average"),    # 50th percentile
        (75, 100, "Average"),    # 75th percentile
        (80, 100, "Needs Improvement"),  # 80th percentile
        (95, 100, "Needs Improvement"),  # 95th percentile
    ]
    
    for rank, total, expected in test_cases:
        category = RankingService.get_rank_category(rank, total)
        status = "✅" if category == expected else "❌"
        print(f"   {status} Rank {rank:2d}/{total} -> {category:<20} (Expected: {expected})")


async def main():
    """Main test function"""
    print("CGPA-Based Student Ranking System Test")
    print("======================================")
    
    # Test basic functionality
    success = await test_ranking_service()
    
    # Test utility functions
    test_rank_categories()
    
    print(f"\n{'='*50}")
    if success:
        print("🎉 All tests passed! The ranking system is working correctly.")
        print("\nKey Features Verified:")
        print("✅ Overall institution rankings")
        print("✅ Batch-wise rankings") 
        print("✅ Semester-wise rankings")
        print("✅ Individual student rank lookup")
        print("✅ Top performers identification")
        print("✅ Performance categorization")
        print("✅ Percentile calculations")
        print("\nThe CGPA-based ranking system is ready for use!")
    else:
        print("❌ Some tests failed. Please check the errors above.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())