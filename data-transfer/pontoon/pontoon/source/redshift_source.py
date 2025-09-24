from pontoon.source.postgresql_source import PostgreSQLSource


class RedshiftSource(PostgreSQLSource):
    """Redshift-specific implementation of SQLSource
    
    Inherits from PostgreSQLSource since Redshift uses the PostgreSQL protocol
    and wire format. The connection string format, authentication method (basic),
    and namespace extraction logic are identical to PostgreSQL.
    
    This class overrides the authentication validation to provide Redshift-specific
    error messages and can be extended in the future if Redshift-specific behaviors
    are needed (e.g., specific query optimizations, column type handling, etc.).
    """
    
    def _validate_auth_type(self, auth_type: str) -> None:
        """Validate authentication type for Redshift - only 'basic' is supported"""
        if auth_type != 'basic':
            raise ValueError(f"Redshift source only supports 'basic' authentication, got '{auth_type}'")