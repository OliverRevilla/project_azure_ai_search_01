
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    BinaryQuantizationCompression,
    HnswAlgorithmConfiguration,
    HnswParameters,
    ScalarQuantizationCompression,
    ScalarQuantizationParameters,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SimpleField,
    VectorSearch,
    VectorSearchProfile,
    VectorSearchCompressionRescoreStorageMethod,
    RescoringOptions
    )
from azure.core.exceptions import ResourceExistsError


class AzureSearchIndexManager:
    def __init__(self, service_endpoint: str, credential: str, index_name_prefix: str, vector_dimensions: int):
        self.client = SearchIndexClient(endpoint=service_endpoint, credential=credential)
        self.index_name_prefix = index_name_prefix
        self.vector_dimensions = vector_dimensions

    def _create_base_fields(self, stored_embedding=True):
        return [
            SimpleField(name="id", type=SearchFieldDataType.String, key=True),
            SearchField(name="title", type=SearchFieldDataType.String, searchable=True),
            SearchField(name="content", type=SearchFieldDataType.String, searchable=True),
            SearchField(
                name="embedding",
                type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                searchable=True,
                vector_search_dimensions=self.vector_dimensions,
                vector_search_profile_name="default-profile",
                stored=stored_embedding,
            ),
        ]

    def _create_compression_config(
        self,
        config_type: str,
        truncate_dims: int = None,
        discard_originals: bool = False,
        oversample_ratio: int = 10,
    ):
        """
        Creates a compression configuration based on the scenario.
        """
        compression_name = f"{config_type}-compression"

        # Determine the storage method based on whether originals are discarded
        rescore_storage_method = (
            VectorSearchCompressionRescoreStorageMethod.DISCARD_ORIGINALS
            if discard_originals
            else VectorSearchCompressionRescoreStorageMethod.PRESERVE_ORIGINALS
        )

        # Enable rescoring only if originals are preserved
        enable_rescoring = not discard_originals

        # Configure rescoring options
        rescoring_options = RescoringOptions(
            enable_rescoring=enable_rescoring,
            default_oversampling=oversample_ratio if enable_rescoring else None,
            rescore_storage_method=rescore_storage_method,
        )

        # Base parameters for compression
        base_params = {
            "compression_name": compression_name,
            "rescoring_options": rescoring_options,
            # Explicitly set deprecated parameters to None
            "rerank_with_original_vectors": None,
            "default_oversampling": None,
        }

        # Add truncation dimension if specified
        if truncate_dims:
            base_params["truncation_dimension"] = truncate_dims

        # Create the appropriate compression object
        if config_type == "scalar":
            compression = ScalarQuantizationCompression(
                parameters=ScalarQuantizationParameters(quantized_data_type="int8"),
                **base_params,
            )
        elif config_type == "binary":
            compression = BinaryQuantizationCompression(
                **base_params,
            )
        else:
            compression = None

        return compression

    def _create_vector_search_config(self, compression_config=None):
        """
        Creates the VectorSearch configuration, including algorithm and compression settings.
        """
        # Define the HNSW algorithm configuration
        algorithm_config = HnswAlgorithmConfiguration(
            name="hnsw-config",
            kind="hnsw",
            parameters=HnswParameters(
                m=4,
                ef_construction=400,
                ef_search=500,
                metric="cosine"
            )
        )

        # Define the VectorSearchProfile
        profiles = [
            VectorSearchProfile(
                name="default-profile",
                algorithm_configuration_name=algorithm_config.name,
                compression_name=compression_config.compression_name if compression_config else None,
            )
        ]

        # Assemble the VectorSearch configuration
        vector_search = VectorSearch(
            profiles=profiles,
            algorithms=[algorithm_config],
            compressions=[compression_config] if compression_config else None,
        )

        return vector_search

    def create_index(self, scenario: dict):
        """
        Creates or updates an index based on the provided scenario.
        """
        index_name = f"{self.index_name_prefix}-{scenario['name']}"

        # Use the 'stored_embedding' value from the scenario
        stored_embedding = scenario.get('stored_embedding', True)

        # Create base fields with the stored_embedding flag
        fields = self._create_base_fields(stored_embedding=stored_embedding)

        # Create compression configuration if specified
        compression_config = None
        if scenario["compression_type"]:
            compression_config = self._create_compression_config(
                config_type=scenario["compression_type"],
                truncate_dims=scenario.get("truncate_dims"),
                discard_originals=scenario.get("discard_originals", False),
            )

        # Create vector search configuration
        vector_search = self._create_vector_search_config(compression_config)

        # Define the SearchIndex
        index = SearchIndex(
            name=index_name,
            fields=fields,
            vector_search=vector_search,
        )

        # Create or update the index
        try:
            self.client.create_or_update_index(index)
        except ResourceExistsError:
            print(f"Index {index_name} already exists.")
        except Exception as e:
            if e.message and "already exists" in e.message:
                print(f"Index {index_name} already exists.")
            else:
                print(f"Error creating index {index_name}: {type(e)} - {str(e)}")
    
        # Return the index name
        return index_name