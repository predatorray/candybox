import { describe, it, expect } from 'vitest';
import { formatBytes, formatCount, formatRelativeTime } from './format';

describe('formatBytes', () => {
  it('returns em-dash for nullish values', () => {
    expect(formatBytes(undefined)).toBe('—');
    expect(formatBytes(null)).toBe('—');
  });
  it('keeps raw bytes under 1 KiB', () => {
    expect(formatBytes(0)).toBe('0 B');
    expect(formatBytes(1023)).toBe('1023 B');
  });
  it('scales into KiB/MiB/GiB with reasonable precision', () => {
    expect(formatBytes(1024)).toBe('1.00 KiB');
    expect(formatBytes(1536)).toBe('1.50 KiB');
    expect(formatBytes(1024 * 1024)).toBe('1.00 MiB');
    expect(formatBytes(1024 * 1024 * 1024 * 12.5)).toBe('12.5 GiB');
  });
});

describe('formatCount', () => {
  it('thousands-separates', () => {
    expect(formatCount(1234567)).toBe('1,234,567');
  });
  it('handles nullish', () => {
    expect(formatCount(undefined)).toBe('—');
  });
});

describe('formatRelativeTime', () => {
  it('treats the present as "0s ago"', () => {
    expect(formatRelativeTime(Date.now())).toMatch(/^[01]s ago$/);
  });
});
