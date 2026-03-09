import difflib
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class PrepService:
    @staticmethod
    def match_columns(
        file_columns: List[Dict[str, str]], 
        template_columns: List[Dict[str, str]], 
        threshold: float = 0.6
    ) -> List[Dict[str, Any]]:
        """
        Match file columns to template columns using fuzzy logic.
        
        file_columns: [{"name": "col1", "type": "TEXT"}, ...]
        template_columns: [{"name": "standard_col", "type": "TEXT"}, ...]
        """
        matches = []
        unmatched_file_cols = [c["name"] for c in file_columns]
        
        for t_col in template_columns:
            t_name = t_col["name"]
            t_type = t_col.get("type", "TEXT").upper()
            
            # Find best fuzzy match in file columns
            best_match = None
            highest_score = 0.0
            
            # Simple fuzzy matching using difflib
            for f_col in file_columns:
                f_name = f_col["name"]
                # Similarity score (0 to 1)
                score = difflib.SequenceMatcher(None, t_name.lower(), f_name.lower()).ratio()
                
                # Check for exact name match (bonus)
                if t_name.lower() == f_name.lower():
                    score = 1.0
                
                # Preliminary type compatibility check (Optional but good)
                # For now, we allow any type but could penalize score for bad matches
                
                if score >= threshold and score > highest_score:
                    highest_score = score
                    best_match = f_name
            
            if best_match:
                matches.append({
                    "template_column": t_name,
                    "file_column": best_match,
                    "score": round(highest_score, 2),
                    "target_type": t_type,
                    "status": "matched" if highest_score > 0.8 else "suggested"
                })
                if best_match in unmatched_file_cols:
                    unmatched_file_cols.remove(best_match)
            else:
                matches.append({
                    "template_column": t_name,
                    "file_column": None,
                    "score": 0,
                    "target_type": t_type,
                    "status": "missing"
                })
        
        # Add remaining file columns as unmatched
        for f_name in unmatched_file_cols:
            matches.append({
                "template_column": None,
                "file_column": f_name,
                "score": 0,
                "target_type": None,
                "status": "unmapped"
            })
            
        return matches

    @staticmethod
    def get_column_similarity(name1: str, name2: str) -> float:
        return difflib.SequenceMatcher(None, name1.lower(), name2.lower()).ratio()
