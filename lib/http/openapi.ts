export function generateOpenApiSpec(basePath: string = ''): object {
  return {
    openapi: '3.0.3',
    info: {
      title: 'Coldbase HTTP API',
      description: 'REST API for Coldbase - a lightweight serverless database for cloud storage',
      version: '1.0.0'
    },
    servers: [
      { url: basePath || '/', description: 'Current server' }
    ],
    components: {
      securitySchemes: {
        bearerAuth: {
          type: 'http',
          scheme: 'bearer',
          description: 'API key with reader, editor, or admin role'
        }
      },
      schemas: {
        Error: {
          type: 'object',
          properties: {
            error: { type: 'string' }
          },
          required: ['error']
        },
        Pagination: {
          type: 'object',
          properties: {
            limit: { type: 'integer' },
            offset: { type: 'integer' },
            hasMore: { type: 'boolean' }
          }
        },
        Document: {
          type: 'object',
          additionalProperties: true
        },
        TransactionOperation: {
          type: 'object',
          properties: {
            type: { type: 'string', enum: ['put', 'delete'] },
            collection: { type: 'string' },
            id: { type: 'string' },
            data: { type: 'object' }
          },
          required: ['type', 'collection', 'id']
        },
        VectorSearchRequest: {
          type: 'object',
          properties: {
            vector: { type: 'array', items: { type: 'number' } },
            k: { type: 'integer', default: 10, maximum: 100, description: 'Number of results (max: 100, configurable)' },
            metric: { type: 'string', enum: ['cosine', 'euclidean', 'dotProduct'], default: 'cosine' }
          },
          required: ['vector']
        },
        CollectionStats: {
          type: 'object',
          properties: {
            collection: { type: 'string' },
            count: { type: 'integer' },
            size: { type: 'integer' }
          }
        }
      }
    },
    security: [{ bearerAuth: [] }],
    paths: {
      '/data/{collection}': {
        get: {
          tags: ['Data'],
          summary: 'List documents',
          description: 'List and query documents in a collection. Supports basic filtering via query params (e.g., ?active=true&role=admin). Filters are applied before the custom query function.',
          parameters: [
            { name: 'collection', in: 'path', required: true, schema: { type: 'string' } },
            { name: 'query', in: 'query', schema: { type: 'string' }, description: 'Custom query string passed to query function (alias: q)' },
            { name: 'q', in: 'query', schema: { type: 'string' }, description: 'Custom query string (alias for query)' },
            { name: 'limit', in: 'query', schema: { type: 'integer', default: 20, maximum: 100 }, description: 'Max results per page (default: 20, max: 100, configurable)' },
            { name: 'offset', in: 'query', schema: { type: 'integer', default: 0 } },
            { name: 'prefix', in: 'query', schema: { type: 'string' }, description: 'Filter by ID prefix' },
            { name: '*', in: 'query', schema: { type: 'string' }, description: 'Any other param is used as prop=value filter (e.g., ?active=true&role=admin)' }
          ],
          responses: {
            '200': {
              description: 'List of documents',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      data: { type: 'array', items: { $ref: '#/components/schemas/Document' } },
                      pagination: { $ref: '#/components/schemas/Pagination' }
                    }
                  }
                }
              }
            },
            '401': { description: 'Unauthorized', content: { 'application/json': { schema: { $ref: '#/components/schemas/Error' } } } },
            '403': { description: 'Forbidden', content: { 'application/json': { schema: { $ref: '#/components/schemas/Error' } } } }
          }
        }
      },
      '/data/{collection}/{id}': {
        get: {
          tags: ['Data'],
          summary: 'Get document',
          description: 'Get a single document by ID',
          parameters: [
            { name: 'collection', in: 'path', required: true, schema: { type: 'string' } },
            { name: 'id', in: 'path', required: true, schema: { type: 'string' } }
          ],
          responses: {
            '200': {
              description: 'Document found',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      data: { $ref: '#/components/schemas/Document' }
                    }
                  }
                }
              }
            },
            '404': { description: 'Not found', content: { 'application/json': { schema: { $ref: '#/components/schemas/Error' } } } }
          }
        },
        put: {
          tags: ['Data'],
          summary: 'Create or update document',
          description: 'Create or update a document by ID',
          parameters: [
            { name: 'collection', in: 'path', required: true, schema: { type: 'string' } },
            { name: 'id', in: 'path', required: true, schema: { type: 'string' } }
          ],
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: { $ref: '#/components/schemas/Document' }
              }
            }
          },
          responses: {
            '200': {
              description: 'Document saved',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      data: { $ref: '#/components/schemas/Document' }
                    }
                  }
                }
              }
            }
          }
        },
        delete: {
          tags: ['Data'],
          summary: 'Delete document',
          description: 'Delete a document by ID',
          parameters: [
            { name: 'collection', in: 'path', required: true, schema: { type: 'string' } },
            { name: 'id', in: 'path', required: true, schema: { type: 'string' } }
          ],
          responses: {
            '200': {
              description: 'Document deleted',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      data: {
                        type: 'object',
                        properties: {
                          id: { type: 'string' },
                          deleted: { type: 'boolean' }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      },
      '/data/{collection}/search': {
        post: {
          tags: ['Data'],
          summary: 'Vector search',
          description: 'Search for similar vectors in a vector collection',
          parameters: [
            { name: 'collection', in: 'path', required: true, schema: { type: 'string' } }
          ],
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: { $ref: '#/components/schemas/VectorSearchRequest' }
              }
            }
          },
          responses: {
            '200': {
              description: 'Search results',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      data: {
                        type: 'array',
                        items: {
                          type: 'object',
                          properties: {
                            id: { type: 'string' },
                            score: { type: 'number' },
                            data: { $ref: '#/components/schemas/Document' }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      },
      '/data/tx': {
        post: {
          tags: ['Data'],
          summary: 'Execute transaction',
          description: 'Execute multiple operations in a transaction (saga pattern)',
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  properties: {
                    operations: {
                      type: 'array',
                      items: { $ref: '#/components/schemas/TransactionOperation' },
                      maxItems: 50,
                      description: 'Operations to execute (max: 50, configurable)'
                    }
                  },
                  required: ['operations']
                }
              }
            }
          },
          responses: {
            '200': {
              description: 'Transaction result',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      success: { type: 'boolean' },
                      results: {
                        type: 'array',
                        items: {
                          type: 'object',
                          properties: {
                            type: { type: 'string' },
                            collection: { type: 'string' },
                            id: { type: 'string' },
                            ok: { type: 'boolean' }
                          }
                        }
                      },
                      error: { type: 'string' },
                      compensated: { type: 'boolean' }
                    }
                  }
                }
              }
            }
          }
        }
      },
      '/admin/collections': {
        get: {
          tags: ['Admin'],
          summary: 'List collections',
          description: 'List all collections in the database',
          responses: {
            '200': {
              description: 'List of collection names',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      data: { type: 'array', items: { type: 'string' } }
                    }
                  }
                }
              }
            }
          }
        }
      },
      '/admin/{collection}/stats': {
        get: {
          tags: ['Admin'],
          summary: 'Collection stats',
          description: 'Get statistics for a collection',
          parameters: [
            { name: 'collection', in: 'path', required: true, schema: { type: 'string' } }
          ],
          responses: {
            '200': {
              description: 'Collection statistics',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      data: { $ref: '#/components/schemas/CollectionStats' }
                    }
                  }
                }
              }
            }
          }
        }
      },
      '/admin/{collection}/compact': {
        post: {
          tags: ['Admin'],
          summary: 'Compact collection',
          description: 'Trigger compaction to merge mutation files',
          parameters: [
            { name: 'collection', in: 'path', required: true, schema: { type: 'string' } }
          ],
          responses: {
            '200': {
              description: 'Compaction triggered',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      data: {
                        type: 'object',
                        properties: {
                          collection: { type: 'string' },
                          operation: { type: 'string' },
                          success: { type: 'boolean' }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      },
      '/admin/{collection}/vacuum': {
        post: {
          tags: ['Admin'],
          summary: 'Vacuum collection',
          description: 'Trigger vacuum to deduplicate and clean up',
          parameters: [
            { name: 'collection', in: 'path', required: true, schema: { type: 'string' } }
          ],
          responses: {
            '200': {
              description: 'Vacuum triggered',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      data: {
                        type: 'object',
                        properties: {
                          collection: { type: 'string' },
                          operation: { type: 'string' },
                          success: { type: 'boolean' }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    tags: [
      { name: 'Data', description: 'Data operations (CRUD, search, transactions)' },
      { name: 'Admin', description: 'Administrative operations (maintenance, stats)' }
    ]
  }
}
