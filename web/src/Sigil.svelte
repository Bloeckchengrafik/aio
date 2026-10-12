<script lang="ts">
  let { size = 40 }: { size?: number } = $props();
</script>

<svg
  width={size}
  height={size}
  viewBox="0 0 64 64"
  fill="none"
  aria-hidden="true"
>
  <path
    d="M32 3 43 21 61 32 43 43 32 61 21 43 3 32 21 21Z"
    stroke="currentColor"
    stroke-width="1"
    opacity=".5"
  />
  <circle cx="32" cy="32" r="19" stroke="currentColor" stroke-width=".8" />
  <path
    d="M32 11V53M11 32H53M19 19 45 45M19 45 45 19"
    stroke="currentColor"
    stroke-width=".7"
    opacity=".45"
  />
  <path
    d="M32 18 36 28 46 32 36 36 32 46 28 36 18 32 28 28Z"
    fill="currentColor"
  />
  <circle cx="32" cy="5" r="2" fill="currentColor" /><circle
    cx="59"
    cy="32"
    r="2"
    fill="currentColor"
  /><circle cx="32" cy="59" r="2" fill="currentColor" /><circle
    cx="5"
    cy="32"
    r="2"
    fill="currentColor"
  />
</svg>
