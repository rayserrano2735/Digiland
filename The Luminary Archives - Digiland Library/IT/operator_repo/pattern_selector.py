#!/usr/bin/env python3
# Filename: pattern_selector.py
"""
Interactive Pattern Selector
Helps you choose the right pattern for your data engineering problem
Place in tools/ directory
"""

import json
from typing import Dict, List, Optional
from pathlib import Path
import textwrap

class PatternSelector:
    def __init__(self):
        self.patterns = self._load_pattern_database()
        
    def _load_pattern_database(self) -> Dict:
        """Load pattern metadata and recommendations."""
        return {
            "sql_patterns": {
                "table_comparison": {
                    "name": "Table Comparison",
                    "path": "sql-patterns/table-comparison/",
                    "files": ["compare_tables_dynamic.sql", "column_mapper.py"],
                    "use_cases": [
                        "Validate data migrations",
                        "Compare staging vs production",
                        "Find differences between tables",
                        "Quality assurance checks",
                        "Audit data changes"
                    ],
                    "keywords": ["compare", "diff", "difference", "validation", "migration", "match"],
                    "complexity": "Intermediate",
                    "time_to_implement": "30 minutes"
                },
                "window_functions": {
                    "name": "Window Functions",
                    "path": "sql-patterns/window-functions/",
                    "files": ["window_functions.sql"],
                    "use_cases": [
                        "Calculate running totals",
                        "Ranking and top-N queries",
                        "Moving averages",
                        "Year-over-year comparisons",
                        "Deduplication",
                        "Gap and island problems"
                    ],
                    "keywords": ["rank", "running total", "moving average", "lag", "lead", "row_number"],
                    "complexity": "Intermediate",
                    "time_to_implement": "15 minutes"
                },
                "pivot_unpivot": {
                    "name": "Pivot/Unpivot",
                    "path": "sql-patterns/pivot-unpivot/",
                    "files": ["pivot_examples.sql", "unpivot_examples.sql"],
                    "use_cases": [
                        "Create cross-tab reports",
                        "Transform rows to columns",
                        "Reshape data for visualization",
                        "Convert EAV models",
                        "Generate matrix reports"
                    ],
                    "keywords": ["pivot", "unpivot", "crosstab", "transpose", "reshape"],
                    "complexity": "Intermediate",
                    "time_to_implement": "20 minutes"
                },
                "data_quality": {
                    "name": "Data Quality Checks",
                    "path": "sql-patterns/data-quality-checks/",
                    "files": ["data_quality_checks.sql"],
                    "use_cases": [
                        "Find NULL values",
                        "Detect duplicates",
                        "Validate foreign keys",
                        "Check data freshness",
                        "Find outliers",
                        "Validate business rules"
                    ],
                    "keywords": ["quality", "validation", "null", "duplicate", "outlier", "freshness"],
                    "complexity": "Beginner",
                    "time_to_implement": "15 minutes"
                }
            },
            "spark_patterns": {
                "optimizations": {
                    "name": "Spark Optimizations",
                    "path": "spark-patterns/optimizations/",
                    "files": ["spark_optimizations.py"],
                    "use_cases": [
                        "Optimize slow Spark jobs",
                        "Handle data skew",
                        "Implement broadcast joins",
                        "Optimize partitioning",
                        "Reduce shuffles",
                        "Cache management"
                    ],
                    "keywords": ["spark", "optimization", "performance", "broadcast", "partition", "skew"],
                    "complexity": "Advanced",
                    "time_to_implement": "1 hour"
                }
            },
            "dbt_patterns": {
                "macros": {
                    "name": "DBT Macros",
                    "path": "dbt-patterns/macros/",
                    "files": ["dbt_macros.sql"],
                    "use_cases": [
                        "Create reusable SQL",
                        "Generate surrogate keys",
                        "Implement incremental models",
                        "Dynamic pivoting",
                        "Custom tests",
                        "Schema management"
                    ],
                    "keywords": ["dbt", "macro", "reusable", "incremental", "test"],
                    "complexity": "Intermediate",
                    "time_to_implement": "30 minutes"
                }
            }
        }
    
    def search_by_keyword(self, keyword: str) -> List[Dict]:
        """Find patterns matching a keyword."""
        keyword = keyword.lower()
        matches = []
        
        for category, patterns in self.patterns.items():
            for pattern_id, pattern in patterns.items():
                # Check pattern keywords
                if any(keyword in kw for kw in pattern["keywords"]):
                    matches.append({**pattern, "category": category, "id": pattern_id})
                # Check use cases
                elif any(keyword in use_case.lower() for use_case in pattern["use_cases"]):
                    matches.append({**pattern, "category": category, "id": pattern_id})
        
        return matches
    
    def get_pattern_by_problem(self, problem_type: str) -> List[Dict]:
        """Get recommended patterns for a problem type."""
        problem_map = {
            "comparison": ["table_comparison"],
            "quality": ["data_quality", "table_comparison"],
            "performance": ["window_functions", "optimizations"],
            "reporting": ["pivot_unpivot", "window_functions"],
            "migration": ["table_comparison", "data_quality"],
            "testing": ["data_quality", "macros"],
            "analytics": ["window_functions", "pivot_unpivot"],
            "etl": ["macros", "data_quality", "optimizations"]
        }
        
        recommendations = []
        problem_type = problem_type.lower()
        
        if problem_type in problem_map:
            pattern_ids = problem_map[problem_type]
            for category, patterns in self.patterns.items():
                for pattern_id, pattern in patterns.items():
                    if pattern_id in pattern_ids:
                        recommendations.append({
                            **pattern, 
                            "category": category, 
                            "id": pattern_id
                        })
        
        return recommendations
    
    def interactive_selector(self):
        """Interactive CLI for pattern selection."""
        print("\n" + "="*60)
        print("   DATA ENGINEERING PATTERN SELECTOR")
        print("="*60)
        
        print("\nHow would you like to find a pattern?")
        print("1. Search by keyword")
        print("2. Browse by problem type")
        print("3. List all patterns")
        print("4. Get pattern by complexity level")
        print("5. Exit")
        
        choice = input("\nEnter choice (1-5): ").strip()
        
        if choice == "1":
            self._search_workflow()
        elif choice == "2":
            self._problem_workflow()
        elif choice == "3":
            self._list_all_patterns()
        elif choice == "4":
            self._complexity_workflow()
        elif choice == "5":
            print("Goodbye!")
            return
        else:
            print("Invalid choice. Please try again.")
            self.interactive_selector()
    
    def _search_workflow(self):
        """Keyword search workflow."""
        keyword = input("\nEnter search keyword: ").strip()
        matches = self.search_by_keyword(keyword)
        
        if matches:
            print(f"\nFound {len(matches)} pattern(s) matching '{keyword}':\n")
            self._display_patterns(matches)
        else:
            print(f"\nNo patterns found for '{keyword}'")
            print("Try keywords like: compare, pivot, quality, rank, optimization")
    
    def _problem_workflow(self):
        """Problem type workflow."""
        print("\nSelect problem type:")
        print("1. Data Comparison/Validation")
        print("2. Data Quality Issues")
        print("3. Performance Optimization")
        print("4. Reporting/Analytics")
        print("5. Data Migration")
        print("6. Testing Data Pipelines")
        print("7. ETL Development")
        
        problem_map = {
            "1": "comparison",
            "2": "quality",
            "3": "performance",
            "4": "reporting",
            "5": "migration",
            "6": "testing",
            "7": "etl"
        }
        
        choice = input("\nEnter choice (1-7): ").strip()
        
        if choice in problem_map:
            problem_type = problem_map[choice]
            recommendations = self.get_pattern_by_problem(problem_type)
            
            if recommendations:
                print(f"\nRecommended patterns for {problem_type}:\n")
                self._display_patterns(recommendations)
            else:
                print("\nNo specific patterns found for this problem type.")
        else:
            print("Invalid choice.")
    
    def _complexity_workflow(self):
        """Filter by complexity level."""
        print("\nSelect complexity level:")
        print("1. Beginner")
        print("2. Intermediate")
        print("3. Advanced")
        
        level_map = {"1": "Beginner", "2": "Intermediate", "3": "Advanced"}
        choice = input("\nEnter choice (1-3): ").strip()
        
        if choice in level_map:
            level = level_map[choice]
            patterns = []
            
            for category, pattern_list in self.patterns.items():
                for pattern_id, pattern in pattern_list.items():
                    if pattern["complexity"] == level:
                        patterns.append({**pattern, "category": category, "id": pattern_id})
            
            if patterns:
                print(f"\n{level} level patterns:\n")
                self._display_patterns(patterns)
            else:
                print(f"\nNo patterns found for {level} level.")
        else:
            print("Invalid choice.")
    
    def _list_all_patterns(self):
        """Display all available patterns."""
        print("\nAll Available Patterns:")
        print("-" * 60)
        
        for category, patterns in self.patterns.items():
            print(f"\n{category.upper().replace('_', ' ')}:")
            for pattern_id, pattern in patterns.items():
                print(f"  • {pattern['name']} ({pattern['complexity']})")
                print(f"    Path: {pattern['path']}")
                print(f"    Time: {pattern['time_to_implement']}")
    
    def _display_patterns(self, patterns: List[Dict]):
        """Display pattern information."""
        for i, pattern in enumerate(patterns, 1):
            print(f"{i}. {pattern['name']}")
            print(f"   Category: {pattern['category'].replace('_', ' ').title()}")
            print(f"   Complexity: {pattern['complexity']}")
            print(f"   Path: {pattern['path']}")
            print(f"   Time to implement: {pattern['time_to_implement']}")
            print(f"   Files: {', '.join(pattern['files'])}")
            print(f"   Use cases:")
            for use_case in pattern['use_cases'][:3]:
                print(f"     - {use_case}")
            print()
        
        # Offer to see more details
        if patterns:
            choice = input("Enter pattern number for more details (or 'q' to quit): ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(patterns):
                self._show_pattern_details(patterns[int(choice) - 1])
    
    def _show_pattern_details(self, pattern: Dict):
        """Show detailed pattern information."""
        print("\n" + "="*60)
        print(f"  {pattern['name'].upper()}")
        print("="*60)
        
        print(f"\nLocation: {pattern['path']}")
        print(f"Complexity: {pattern['complexity']}")
        print(f"Implementation Time: {pattern['time_to_implement']}")
        
        print("\nFiles to review:")
        for file in pattern['files']:
            print(f"  - {file}")
        
        print("\nUse Cases:")
        for use_case in pattern['use_cases']:
            print(f"  • {use_case}")
        
        print("\nNext Steps:")
        print(f"1. Navigate to: {pattern['path']}")
        print(f"2. Review the README.md for detailed documentation")
        print(f"3. Check the main solution file: {pattern['files'][0]}")
        print(f"4. Look at examples/ folder for real-world usage")
        
        # Generate sample command
        print("\nQuick Start Command:")
        print(f"  cd {pattern['path']}")
        if pattern['files'][0].endswith('.sql'):
            print(f"  cat {pattern['files'][0]}")
        elif pattern['files'][0].endswith('.py'):
            print(f"  python {pattern['files'][0]}")

def main():
    """Main entry point."""
    import sys
    
    selector = PatternSelector()
    
    if len(sys.argv) > 1:
        # Command line mode
        keyword = " ".join(sys.argv[1:])
        print(f"\nSearching for patterns matching: '{keyword}'")
        matches = selector.search_by_keyword(keyword)
        
        if matches:
            print(f"\nFound {len(matches)} match(es):")
            for match in matches:
                print(f"\n• {match['name']}")
                print(f"  Path: {match['path']}")
                print(f"  Complexity: {match['complexity']}")
        else:
            print(f"No patterns found for '{keyword}'")
            print("\nTry: python pattern_selector.py -i for interactive mode")
    else:
        # Interactive mode
        selector.interactive_selector()

if __name__ == "__main__":
    main()