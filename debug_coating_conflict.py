#!/usr/bin/env python3
"""
Debug the coating conflict issue with Z100MB/GI40/40 vs Z100MB.
"""

import pandas as pd
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from emw_convertor.pipeline.extractor import ExtractorRunner
from emw_convertor.pipeline.grade_extractor import GradeExtractor
from emw_convertor.pipeline.dimension_extractor import DimensionExtractor
from emw_convertor.pipeline.coating_treatment import CoatingTreatmentExtractor
from emw_convertor.utils.helper import load_schema_list
from emw_convertor import grades_schema, coating_schema


def debug_coating_conflict():
    """Debug why Z100MB/GI40/40 should not match while Z100MB should match."""
    print("🔍 DEBUGGING COATING CONFLICT ISSUE")
    print("=" * 60)

    # The specific problematic cases
    test_cases = [
        "C 1,7x380 CR210LA Z100MB/GI40/40",  # Should NOT match (conflicting coatings)
        "C 1,3x945 CR4 Z100MB",  # Should match (single coating)
    ]

    print("Test cases:")
    for i, case in enumerate(test_cases):
        print(f"  Case {i+1}: '{case}'")

    print(f"\nExpected behavior:")
    print(
        f"  Case 1: Should NOT match - Z100MB/GI40/40 has conflicting coating patterns"
    )
    print(f"  Case 2: Should match - Z100MB is a valid single coating")

    # Check coating schema for Z and GI patterns
    print(f"\n🎨 Checking coating schema:")
    z_patterns = []
    gi_patterns = []

    for entry in coating_schema:
        if "coating" in entry:
            prefix = entry.get("prefix_coating", "")
            if prefix == "Z":
                z_patterns.extend(entry["coating"])
            elif prefix == "GI":
                gi_patterns.extend(entry["coating"])

    print(f"  Z coatings: {z_patterns}")
    print(f"  GI coatings: {gi_patterns}")

    # Check if 100 and 40/40 are in the respective lists
    print(f"\n🔬 Pattern analysis:")
    print(f"  '100' in Z coatings: {'100' in z_patterns}")
    print(f"  '40/40' in GI coatings: {'40/40' in gi_patterns}")

    # Test coating extraction manually
    coating_extractor = CoatingTreatmentExtractor(coating_schema)

    test_coating_strings = [
        ("z100mb/gi40/40", "CR210LA"),  # Conflicting pattern
        ("z100mb", "CR4"),  # Single pattern
        ("z100", "CR4"),  # Just the coating part
        ("gi40/40", "CR210LA"),  # Just the GI part
    ]

    print(f"\n🧪 Manual coating extraction tests:")
    for coating_str, grade in test_coating_strings:
        print(f"\n  Testing: '{coating_str}' with grade '{grade}'")
        try:
            result = coating_extractor.extract_coating_treatment(coating_str, grade)
            print(f"    Result: {result}")
            if len(result) == 4:
                grade_result, coating, treatment, remaining = result
                print(f"      Grade: '{grade_result}'")
                print(f"      Coating: '{coating}'")
                print(f"      Treatment: '{treatment}'")
                print(f"      Remaining: '{remaining}'")
        except Exception as e:
            print(f"    ❌ Error: {e}")


def test_full_extraction():
    """Test full extraction on the problematic cases."""
    print(f"\n🔬 FULL EXTRACTION TEST")
    print("=" * 60)

    test_cases = [
        "C 1,7x380 CR210LA Z100MB/GI40/40",  # Should NOT match
        "C 1,3x945 CR4 Z100MB",  # Should match
    ]

    # Create test DataFrame
    test_df = pd.DataFrame({"Materialkurztext": test_cases})

    # Set up extractors
    header_names = {"grades": "Materialkurztext", "dimensions": "Materialkurztext"}

    grade_extractor = GradeExtractor(
        grade_list=load_schema_list(grades_schema, "base_grade"),
    )

    dimension_extractor = DimensionExtractor(column_name=header_names["dimensions"])

    coating_treatment_extractor = CoatingTreatmentExtractor(
        treatment_dict=coating_schema
    )

    extractor_runner = ExtractorRunner(
        header_names=header_names,
        grade_extractor=grade_extractor,
        dimension_extractor=dimension_extractor,
        coating_treatment_extractor=coating_treatment_extractor,
    )

    # Run extraction
    result_df = extractor_runner.run_extractor(test_df)

    print(f"Results:")
    for idx, row in result_df.iterrows():
        print(f"\nCase {idx+1}: '{row['Materialkurztext']}'")
        print(f"  Grade: '{row.get('Güte_', 'None')}'")
        print(f"  Coating: '{row.get('Auflage_', 'None')}'")
        print(f"  Treatment: '{row.get('Oberfläche_', 'None')}'")
        print(f"  Unmatched: '{row.get('Unmatched_Remainder_', 'None')}'")
        print(f"  Yellow Highlight: {row.get('Highlight_Row_', 'None')}")
        print(f"  Red Highlight: {row.get('Red_Highlight_Row_', 'None')}")

        # Analyze results
        if idx == 0:  # Z100MB/GI40/40 case
            unmatched = row.get("Unmatched_Remainder_", "")
            if unmatched and "gi40/40" in unmatched.lower():
                print(
                    f"  ✅ CORRECT: GI40/40 part not matched (conflicting with Z100MB)"
                )
            elif not unmatched:
                print(f"  ❌ INCORRECT: Everything matched (should have conflict)")
            else:
                print(
                    f"  ⚠️  PARTIAL: Some unmatched content but not the expected pattern"
                )

        elif idx == 1:  # Z100MB case
            coating = row.get("Auflage_", None)
            treatment = row.get("Oberfläche_", None)
            if coating == "100" and treatment == "MBO":
                print(
                    f"  ✅ CORRECT: Z100MB properly extracted as coating=100, treatment=MBO"
                )
            else:
                print(f"  ❌ INCORRECT: Z100MB not properly extracted")


if __name__ == "__main__":
    debug_coating_conflict()
    test_full_extraction()

    print(f"\n" + "=" * 60)
    print("🎯 ANALYSIS COMPLETE")
    print("\nThe issue is likely:")
    print("1. Z100MB/GI40/40 contains conflicting coating prefixes (Z and GI)")
    print(
        "2. The extraction logic may be matching the first pattern and ignoring conflicts"
    )
    print("3. Need to detect and handle conflicting coating patterns")
    print("4. Z100MB alone should work: Z + 100 coating + MB treatment")
