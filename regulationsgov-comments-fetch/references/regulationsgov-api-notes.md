# Regulations.gov Comments API Notes

## Endpoint

- `GET /comments`
- Base URL: `https://api.regulations.gov/v4`
- Authentication: `X-Api-Key: <REGGOV_API_KEY>`

## Filters Used by This Skill

- `filter[lastModifiedDate][ge]`
- `filter[lastModifiedDate][le]`
  - Format: `yyyy-MM-dd HH:mm:ss`
- `filter[postedDate][ge]`
- `filter[postedDate][le]`
  - Format: `yyyy-MM-dd`
- Optional:
  - `filter[agencyId]`
  - `filter[commentOnId]`
  - `filter[searchTerm]`

## Pagination

- `page[size]`: accepted `5` to `250`
- `page[number]`: starts at `1`
- Response `meta` includes pagination state:
  - `hasNextPage`
  - `pageNumber`
  - `pageSize`
  - `totalElements`
  - `totalPages`

## Sorting

- Common values for comments endpoint:
  - `postedDate`
  - `lastModifiedDate`
  - `documentId`
- Descending sort: prefix `-` (example: `-lastModifiedDate`)

## Response Shape (List)

- Top-level object with:
  - `data`: array of comment resources
  - `meta`: pagination + filters + aggregations
- Each `data[]` item generally contains:
  - `id`
  - `type` (expected `comments`)
  - `attributes`
  - `links`

The skill validates this structure and records validation issues when present.
