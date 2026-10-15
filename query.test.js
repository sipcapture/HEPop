import { describe, expect, test } from 'bun:test';
import QueryClient, {
  buildTimestampWhereClause,
  stripTimePredicates,
} from './query.js';

describe('stripTimePredicates', () => {
  test('removes only time comparisons', () => {
    const input = "time >= '2025-01-01' AND host = 'a' AND runtime > 5";
    expect(stripTimePredicates(input)).toBe("host = 'a' AND runtime > 5");
  });
});

describe('buildTimestampWhereClause', () => {
  test('omits open start bound', () => {
    const clause = buildTimestampWhereClause({
      start: null,
      end: 1_700_000_000_000_000_000,
    });
    expect(clause).not.toContain('>=');
    expect(clause).toContain('<=');
  });

  test('omits open end bound', () => {
    const clause = buildTimestampWhereClause({
      start: 1_700_000_000_000_000_000,
      end: null,
    });
    expect(clause).toContain('>=');
    expect(clause).not.toContain('<=');
  });
});

describe('QueryClient.parseQuery', () => {
  test('preserves non-time WHERE clauses', () => {
    const client = new QueryClient('./data', {});
    const parsed = client.parseQuery(`
      SELECT * FROM cpu
      WHERE time >= '2025-02-09T16:00:00' AND host = 'edge'
    `);
    expect(parsed.conditions).toContain("host = 'edge'");
    expect(parsed.conditions.toLowerCase()).not.toContain('time >=');
  });
});
