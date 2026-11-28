import styles from './EnvironmentBanner.module.css';

function textColor(backgroundColor: string) {
  const hex = backgroundColor.slice(1);
  const rgb = hex.length === 3 ? [...hex].map((digit) => digit.repeat(2)).join('') : hex;
  const [red, green, blue] = [0, 2, 4].map((offset) => {
    const channel = parseInt(rgb.slice(offset, offset + 2), 16) / 255;
    return channel <= 0.04045 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4;
  });
  const luminance = 0.2126 * red + 0.7152 * green + 0.0722 * blue;
  // Choose whichever foreground gives the higher WCAG contrast ratio.
  return luminance > 0.179 ? '#000' : '#fff';
}

export const EnvironmentBanner = ({text, color}: {text: string; color: string}) => (
  <div
    className={styles.banner}
    role="note"
    style={{backgroundColor: color, color: textColor(color)}}
  >
    {text}
  </div>
);
