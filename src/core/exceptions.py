"""
Custom exceptions for the VN Finance Dashboard
"""

class DashboardError(Exception):
    """Base exception for dashboard errors"""
    pass

class DataLoadError(DashboardError):
    """Raised when data loading fails"""
    
    def __init__(self, message: str, file_path: str = None, error: Exception = None):
        self.message = message
        self.file_path = file_path
        self.error = error
        
        if file_path:
            message = f"Failed to load data from {file_path}: {message}"
        
        if error:
            message = f"{message} (Original error: {error})"
        
        super().__init__(message)

class APIConnectionError(DashboardError):
    """Raised when API connection fails"""
    
    def __init__(self, message: str, api_provider: str = None, status_code: int = None):
        self.message = message
        self.api_provider = api_provider
        self.status_code = status_code
        
        if api_provider:
            message = f"API connection failed for {api_provider}: {message}"
        
        if status_code:
            message = f"{message} (Status: {status_code})"
        
        super().__init__(message)

class ValidationError(DashboardError):
    """Raised when data validation fails"""
    
    def __init__(self, message: str, field: str = None, value: any = None):
        self.message = message
        self.field = field
        self.value = value
        
        if field:
            message = f"Validation failed for field '{field}': {message}"
        
        if value is not None:
            message = f"{message} (Value: {value})"
        
        super().__init__(message)

class ConfigurationError(DashboardError):
    """Raised when configuration is invalid"""
    
    def __init__(self, message: str, config_path: str = None):
        self.message = message
        self.config_path = config_path
        
        if config_path:
            message = f"Configuration error in {config_path}: {message}"
        
        super().__init__(message)

class CacheError(DashboardError):
    """Raised when cache operations fail"""
    
    def __init__(self, message: str, cache_key: str = None, operation: str = None):
        self.message = message
        self.cache_key = cache_key
        self.operation = operation
        
        if operation and cache_key:
            message = f"Cache {operation} failed for key '{cache_key}': {message}"
        elif operation:
            message = f"Cache {operation} failed: {message}"
        
        super().__init__(message)

class DataProcessingError(DashboardError):
    """Raised when data processing fails"""
    
    def __init__(self, message: str, operation: str = None, data_shape: tuple = None):
        self.message = message
        self.operation = operation
        self.data_shape = data_shape
        
        if operation:
            message = f"Data processing failed during {operation}: {message}"
        
        if data_shape:
            message = f"{message} (Data shape: {data_shape})"
        
        super().__init__(message)
