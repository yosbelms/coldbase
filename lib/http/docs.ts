import { Hono } from 'hono'
import { swaggerUI } from '@hono/swagger-ui'
import { generateOpenApiSpec } from './openapi'

export function createDocsRoutes(): Hono {
  const app = new Hono()

  // Docs routes are public - no authentication required
  // Users can view API documentation to learn about authentication

  // GET / - Swagger UI
  app.get('/', swaggerUI({ url: '/docs/openapi.json' }))

  // GET /openapi.json - OpenAPI spec
  app.get('/openapi.json', (c) => {
    const spec = generateOpenApiSpec()
    return c.json(spec)
  })

  return app
}
