"""
Transaction management for SimplerDB connectors.
"""

import logging
from typing import TYPE_CHECKING, Any, Optional

from .exceptions import TransactionError

if TYPE_CHECKING:
    from .base import DBInterface

logger = logging.getLogger(__name__)

class TransactionContext:
    """
    Context manager for database transactions.
    
    Provides automatic commit/rollback handling within a context block.
    """
    
    def __init__(self, db: 'DBInterface', savepoint: Optional[str] = None):
        """
        Initialize the transaction context.
        
        Args:
            db: Database connector instance
            savepoint: Optional savepoint name for nested transactions
        """
        self.db = db
        self.savepoint = savepoint
        self.savepoint_created = False
        
    def __enter__(self) -> Any:
        """Start a transaction or create a savepoint"""
        try:
            if self.savepoint:
                self._create_savepoint()
                self.savepoint_created = True
            return self
        except Exception as e:
            raise TransactionError(f"Failed to start transaction: {e}") from e
            
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """End the transaction with commit or rollback"""
        if exc_type is None:
            # No exception occurred, commit the transaction
            try:
                if self.savepoint_created:
                    self._release_savepoint()
                else:
                    self.db.commit()
                return True
            except Exception as e:
                logger.error(f"Transaction commit failed: {e}")
                # Try to rollback on commit failure
                try:
                    if self.savepoint_created:
                        self._rollback_to_savepoint()
                    else:
                        self.db.rollback()
                except Exception as rollback_error:
                    logger.error(f"Rollback after commit failure also failed: {rollback_error}")
                # Re-raise the original commit error
                raise TransactionError(f"Transaction commit failed: {e}") from e
        else:
            # Exception occurred, rollback the transaction
            try:
                if self.savepoint_created:
                    self._rollback_to_savepoint()
                else:
                    self.db.rollback()
                logger.warning(f"Transaction rolled back due to: {exc_type.__name__}: {exc_val}")
                return False  # Re-raise the exception
            except Exception as e:
                logger.error(f"Transaction rollback failed: {e}")
                raise TransactionError(f"Transaction rollback failed: {e}") from e
    
    def _create_savepoint(self) -> None:
        """Create a savepoint for nested transactions"""
        if hasattr(self.db, 'create_savepoint'):
            self.db.create_savepoint(self.savepoint)
        else:
            # Fall back to executing a SAVEPOINT statement
            self.db.execute(f"SAVEPOINT {self.savepoint}")
    
    def _release_savepoint(self) -> None:
        """Release a savepoint"""
        if hasattr(self.db, 'release_savepoint'):
            self.db.release_savepoint(self.savepoint)
        else:
            # Fall back to executing a RELEASE SAVEPOINT statement
            self.db.execute(f"RELEASE SAVEPOINT {self.savepoint}")
    
    def _rollback_to_savepoint(self) -> None:
        """Rollback to a savepoint"""
        if hasattr(self.db, 'rollback_to_savepoint'):
            self.db.rollback_to_savepoint(self.savepoint)
        else:
            # Fall back to executing a ROLLBACK TO SAVEPOINT statement
            self.db.execute(f"ROLLBACK TO SAVEPOINT {self.savepoint}")