// frontend/components/ThemeSwitcher.js
import { useTheme } from "./ThemeContext";

export default function ThemeSwitcher() {
  const { theme, toggleTheme } = useTheme();

  return (
    <button
      className="px-3 py-1 rounded border hover:bg-gray-200 dark:hover:bg-gray-700"
      onClick={toggleTheme}
    >
      {theme === "light" ? "🌙 Mode sombre" : "☀️ Mode clair"}
    </button>
  );
}
