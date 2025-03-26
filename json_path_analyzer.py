#!/usr/bin/env python3
"""
JSON Path Analyzer - A helper script to analyze JSON structure and validate mappings.
This script helps identify mismatches between JSON structure and CSV mappings.
"""

import os
import sys
import json
import csv
import argparse
from typing import Dict, List, Any, Set

def analyze_json_structure(json_file: str, mapping_csv: str, output_file: str = None):
    """
    Analyze JSON structure and validate mappings.
    
    Args:
        json_file: Path to JSON file
        mapping_csv: Path to mapping CSV file
        output_file: Optional path to output file for analysis results
    """
    # Load JSON data
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Load field mappings
    with open(mapping_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        field_mappings = list(reader)
    
    # Extract all available paths from JSON
    json_paths = set()
    extract_paths(data, "", json_paths)
    
    # Check mappings against JSON structure
    mapping_paths = set()
    problem_paths = []
    
    for mapping in field_mappings:
        path = mapping['original_path']
        table = mapping['table_name']
        field = mapping['field_name']
        
        mapping_paths.add(path)
        
        # Check if path exists in JSON
        normalized_path = normalize_path(path)
        if normalized_path not in json_paths:
            problem_paths.append({
                'table': table,
                'field': field,
                'path': path,
                'found': False,
                'similar_paths': find_similar_paths(normalized_path, json_paths)
            })
    
    # Prepare output
    output_lines = []
    output_lines.append(f"JSON file: {json_file}")
    output_lines.append(f"Mapping CSV: {mapping_csv}")
    output_lines.append(f"Total JSON paths: {len(json_paths)}")
    output_lines.append(f"Total mapping paths: {len(mapping_paths)}")
    output_lines.append(f"Problem paths: {len(problem_paths)}")
    
    # Add problem paths
    if problem_paths:
        output_lines.append("\nProblem Paths:")
        for problem in problem_paths:
            output_lines.append(f"  Table: {problem['table']}, Field: {problem['field']}")
            output_lines.append(f"  Path: {problem['path']}")
            output_lines.append(f"  Found: {problem['found']}")
            if problem['similar_paths']:
                output_lines.append(f"  Similar paths: {', '.join(problem['similar_paths'][:5])}")
            output_lines.append("")
    
    # Add sample JSON structure
    output_lines.append("\nSample JSON Structure:")
    if 'results' in data and isinstance(data['results'], list) and len(data['results']) > 0:
        sample_item = data['results'][0]
        output_lines.append(json.dumps(sample_item, indent=2)[:1000] + "...")
    else:
        output_lines.append(json.dumps(data, indent=2)[:1000] + "...")
    
    # Generate fixed mapping CSV
    if problem_paths:
        output_lines.append("\nSuggested Fixes:")
        fixed_mappings = suggest_fixes(data, field_mappings, problem_paths)
        if fixed_mappings:
            fixed_csv_path = f"{os.path.splitext(mapping_csv)[0]}_fixed.csv"
            with open(fixed_csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=field_mappings[0].keys())
                writer.writeheader()
                writer.writerows(fixed_mappings)
            output_lines.append(f"Fixed mappings written to {fixed_csv_path}")
            
            # Update output if using file
            if output_file:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write("\n".join(output_lines))
            else:
                print("\n".join(output_lines[-(len(output_lines)-output_lines.index("\nSuggested Fixes:")):]))
    
    # Output to file or console
    output_text = "\n".join(output_lines)
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(output_text)
        print(f"Analysis written to {output_file}")
    else:
        print(output_text)

def extract_paths(obj: Any, current_path: str, paths: Set[str]):
    """
    Recursively extract all paths from JSON object.
    
    Args:
        obj: JSON object or value
        current_path: Current path
        paths: Set to store paths
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            new_path = f"{current_path}.{key}" if current_path else key
            paths.add(new_path)
            extract_paths(value, new_path, paths)
    elif isinstance(obj, list):
        # For arrays, add both the array path and indexed elements
        paths.add(current_path)
        
        for i, item in enumerate(obj):
            # Add path with index
            indexed_path = f"{current_path}[{i}]"
            paths.add(indexed_path)
            extract_paths(item, indexed_path, paths)
            
            # Also add generic [0] index for the first item to match common mapping patterns
            if i == 0:
                generic_path = f"{current_path}[0]"
                paths.add(generic_path)
                extract_paths(item, generic_path, paths)
    else:
        # Primitive value, just add the path
        paths.add(current_path)

def normalize_path(path: str) -> str:
    """
    Normalize a path for comparison.
    
    Args:
        path: Path to normalize
        
    Returns:
        Normalized path
    """
    # Replace indexed references with generic [0]
    import re
    return re.sub(r'\[\d+\]', '[0]', path)

def find_similar_paths(path: str, all_paths: Set[str], max_results: int = 5) -> List[str]:
    """
    Find similar paths to the given path.
    
    Args:
        path: Path to find similar paths for
        all_paths: Set of all available paths
        max_results: Maximum number of results to return
        
    Returns:
        List of similar paths
    """
    # Split path into components
    components = path.split('.')
    
    # Find paths that match some components
    similar_paths = []
    
    for available_path in all_paths:
        avail_components = available_path.split('.')
        
        # Calculate similarity score (higher is more similar)
        score = 0
        for c1 in components:
            for c2 in avail_components:
                if c1 == c2:
                    score += 2
                elif c1 in c2 or c2 in c1:
                    score += 1
        
        if score > 0:
            similar_paths.append((score, available_path))
    
    # Sort by score (highest first) and return paths
    similar_paths.sort(reverse=True)
    return [p[1] for p in similar_paths[:max_results]]

def suggest_fixes(data: Dict, field_mappings: List[Dict], problem_paths: List[Dict]) -> List[Dict]:
    """
    Suggest fixes for common mapping problems.
    
    Args:
        data: JSON data
        field_mappings: Field mappings from CSV
        problem_paths: List of problem paths
        
    Returns:
        List of fixed field mappings
    """
    # Create a copy of mappings to modify
    fixed_mappings = field_mappings.copy()
    
    # Track which mappings need to be updated
    updates = {}
    
    # Common fix: Add 'results.' prefix to paths
    if 'results' in data and isinstance(data['results'], list):
        for problem in problem_paths:
            path = problem['path']
            if not path.startswith('results.') and not path == 'results':
                new_path = f"results.{path}"
                
                # Check if new path with 'results.' prefix exists
                if any(p for p in problem['similar_paths'] if p == new_path or p.startswith(new_path + '.')):
                    print(f"Suggested fix for {problem['table']}.{problem['field']}: {path} -> {new_path}")
                    updates[f"{problem['table']}:{problem['field']}"] = new_path
    
    # Apply updates to mappings
    for i, mapping in enumerate(fixed_mappings):
        key = f"{mapping['table_name']}:{mapping['field_name']}"
        if key in updates:
            fixed_mappings[i] = mapping.copy()
            fixed_mappings[i]['original_path'] = updates[key]
    
    return fixed_mappings

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Analyze JSON structure and validate mappings')
    
    # Input files
    parser.add_argument('--json-file', required=True, help='JSON file to analyze')
    parser.add_argument('--mapping-csv', required=True, help='CSV file containing field mappings')
    parser.add_argument('--output-file', help='Optional output file for analysis results')
    
    args = parser.parse_args()
    
    # Run analysis
    analyze_json_structure(args.json_file, args.mapping_csv, args.output_file)

if __name__ == '__main__':
    main()