extends Control

@onready var metrics_left: RichTextLabel = $Margin/Root/MainColumns/CenterPanel/CenterBox/MetricsRow/MetricsLeft
@onready var metrics_right: RichTextLabel = $Margin/Root/MainColumns/CenterPanel/CenterBox/MetricsRow/MetricsRight
@onready var achievements_scroll: ScrollContainer = $Margin/Root/MainColumns/CenterPanel/CenterBox/AchievementsBarRow/AchievementsScroll
@onready var achievement_groups_root: VBoxContainer = $Margin/Root/MainColumns/CenterPanel/CenterBox/AchievementsBarRow/AchievementsScroll/AchievementGroupsRoot
@onready var launch_now_button: Button = $Margin/Root/MainColumns/CenterPanel/CenterBox/LaunchNow
@onready var risk_badge_label: Label = $Margin/Root/MainColumns/CenterPanel/CenterBox/RiskBadge
@onready var tasks_label: Label = $Margin/Root/MainColumns/SprintPanel/SprintBox/Tasks
@onready var backlog_row: BacklogRow = $Margin/Root/MainColumns/SprintPanel/SprintBox/BacklogScroll/BacklogRow
@onready var slot0: SprintSlot = $Margin/Root/MainColumns/SprintPanel/SprintBox/SlotsRow/Slot0
@onready var slot1: SprintSlot = $Margin/Root/MainColumns/SprintPanel/SprintBox/SlotsRow/Slot1
@onready var slot2: SprintSlot = $Margin/Root/MainColumns/SprintPanel/SprintBox/SlotsRow/Slot2
@onready var event_label: Label = $LegacyEventHost/EventPanel/EventBox/EventScroll/Event
@onready var event_title_label: Label = $LegacyEventHost/EventPanel/EventBox/CastRow/EventTitle
@onready var event_avatar: TextureRect = $LegacyEventHost/EventPanel/EventBox/CastRow/InvestorAvatar
@onready var event_speaker_chip: Label = $LegacyEventHost/EventPanel/EventBox/SpeakerChip
@onready var log_label: Label = $Margin/Root/MainColumns/CenterPanel/CenterBox/LogScroll/Log
@onready var log_scroll: ScrollContainer = $Margin/Root/MainColumns/CenterPanel/CenterBox/LogScroll
@onready var log_toggle_button: Button = $Margin/Root/MainColumns/CenterPanel/CenterBox/AchievementsBarRow/LogToggleBottom
@onready var log_overlay: Control = $LogOverlay
@onready var log_overlay_dialog: PanelContainer = $LogOverlay/Wrap/Dialog
@onready var log_overlay_title: Label = $LogOverlay/Wrap/Dialog/Box/LogHeroRow/LogTitleStack/Title
@onready var log_overlay_hint: Label = $LogOverlay/Wrap/Dialog/Box/LogHeroRow/LogTitleStack/Hint
@onready var log_overlay_scroll: ScrollContainer = $LogOverlay/Wrap/Dialog/Box/Scroll
@onready var log_overlay_body: Label = $LogOverlay/Wrap/Dialog/Box/Scroll/Body
@onready var log_overlay_save_button: Button = $LogOverlay/Wrap/Dialog/Box/Buttons/Save
@onready var log_overlay_close_button: Button = $LogOverlay/Wrap/Dialog/Box/Buttons/Close
@onready var choose_a: Button = $LegacyEventHost/EventPanel/EventBox/EventButtons/ChooseA
@onready var choose_b: Button = $LegacyEventHost/EventPanel/EventBox/EventButtons/ChooseB
@onready var next_week_button: Button = $Margin/Root/MainColumns/SprintPanel/SprintBox/NextWeek
@onready var sprint_panel: PanelContainer = $Margin/Root/MainColumns/SprintPanel
@onready var center_panel: PanelContainer = $Margin/Root/MainColumns/CenterPanel
@onready var event_panel: PanelContainer = $LegacyEventHost/EventPanel
@onready var top_bar: PanelContainer = $Margin/Root/TopBar
@onready var title_label: Label = $Margin/Root/TopBar/TopBarRow/HeaderText/Title
@onready var subtitle_label: Label = $Margin/Root/TopBar/TopBarRow/HeaderText/Subtitle
@onready var metrics_title_label: Label = $Margin/Root/MainColumns/CenterPanel/CenterBox/PulseHeaderRow/MetricsTitle
@onready var pulse_mode_toggle_button: Button = $Margin/Root/MainColumns/CenterPanel/CenterBox/PulseHeaderRow/PulseModeToggle
@onready var slots_hint_label: Label = $Margin/Root/MainColumns/SprintPanel/SprintBox/SlotsHint
@onready var header_avatar: TextureRect = $Margin/Root/TopBar/TopBarRow/HeaderAvatar
@onready var stage_bg_rect: TextureRect = $Background
@onready var bg_music: AudioStreamPlayer = $BgMusic
@onready var game_over_music: AudioStreamPlayer = $GameOverMusic
@onready var stage_intro_art: TextureRect = $StageIntroOverlay/StageIntroArt
@onready var stage_intro_overlay: Control = $StageIntroOverlay
@onready var stage_intro_corner_mascot: TextureRect = $StageIntroOverlay/StageIntroMascot
@onready var stage_intro_dialog: PanelContainer = $StageIntroOverlay/DialogWrap/Dialog
@onready var welcome_intro_page: Control = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/WelcomeIntroPage
@onready var welcome_title: Label = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/WelcomeIntroPage/WelcomeIntroColumn/WelcomeMainRow/WelcomeTextCol/WelcomeTitle
@onready var welcome_body_scroll: ScrollContainer = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/WelcomeIntroPage/WelcomeIntroColumn/WelcomeMainRow/WelcomeTextCol/WelcomeBodyScroll
@onready var welcome_body: Label = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/WelcomeIntroPage/WelcomeIntroColumn/WelcomeMainRow/WelcomeTextCol/WelcomeBodyScroll/WelcomeBody
@onready var welcome_mascot: TextureRect = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/WelcomeIntroPage/WelcomeIntroColumn/WelcomeMainRow/WelcomeMascot
@onready var welcome_next_button: Button = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/WelcomeIntroPage/WelcomeIntroColumn/WelcomeMainRow/WelcomeTextCol/WelcomeButtonRow/WelcomeNextButton
@onready var welcome_stage_card_build: PanelContainer = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/WelcomeIntroPage/WelcomeIntroColumn/WelcomeMainRow/WelcomeTextCol/StageCards/StageCardBuild
@onready var welcome_stage_card_fit: PanelContainer = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/WelcomeIntroPage/WelcomeIntroColumn/WelcomeMainRow/WelcomeTextCol/StageCards/StageCardFit
@onready var welcome_stage_card_scale: PanelContainer = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/WelcomeIntroPage/WelcomeIntroColumn/WelcomeMainRow/WelcomeTextCol/StageCards/StageCardScale
@onready var welcome_alert_block: PanelContainer = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/WelcomeIntroPage/WelcomeIntroColumn/WelcomeMainRow/WelcomeTextCol/AlertBlock
@onready var stage_pill_row: HBoxContainer = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StagePillRow
@onready var stage_pill_build: Label = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StagePillRow/PillBuild
@onready var stage_pill_fit: Label = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StagePillRow/PillFit
@onready var stage_pill_scale: Label = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StagePillRow/PillScale
@onready var stage_pill_sep1: Label = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StagePillRow/Sep1
@onready var stage_pill_sep2: Label = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StagePillRow/Sep2
@onready var stage_intro_page: Control = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StageIntroPage
@onready var stage_intro_preview: TextureRect = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StageIntroPage/StageIntroHero/StageIntroPreview
@onready var stage_intro_text_panel: PanelContainer = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StageIntroPage/StageIntroHero/StageIntroTextPanel
@onready var stage_intro_badge: Label = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StageIntroPage/StageIntroHero/StageIntroTextPanel/StageIntroTextCol/StageBadge
@onready var stage_intro_title: Label = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StageIntroPage/StageIntroHero/StageIntroTextPanel/StageIntroTextCol/StageTitle
@onready var stage_intro_goal: Label = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StageIntroPage/StageIntroHero/StageIntroTextPanel/StageIntroTextCol/StageGoal
@onready var stage_intro_body: Label = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StageIntroPage/StageIntroHero/StageIntroTextPanel/StageIntroTextCol/StageIntro
@onready var stage_intro_start_button: Button = $StageIntroOverlay/DialogWrap/Dialog/DialogBox/StageIntroPage/StageStartButton
@onready var week_summary_overlay: Control = $WeekSummaryOverlay
@onready var week_summary_dialog: PanelContainer = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog
@onready var friday_intro_page: Control = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage
@onready var friday_intro_title: Label = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayIntroTitle
@onready var friday_intro_art: TextureRect = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayIntroHero/FridayPortraitCol/FridayIntroArt
@onready var friday_intro_speaker: Label = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayIntroHero/FridayPortraitCol/FridayIntroSpeaker
@onready var friday_intro_body: RichTextLabel = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayIntroHero/FridayIntroBody
@onready var friday_options_row: Control = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayOptionsRow
@onready var friday_opt_badge_a: Label = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayOptionsRow/FridayOptPanelA/FridayOptStackA/FridayOptBadgeA
@onready var friday_opt_badge_b: Label = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayOptionsRow/FridayOptPanelB/FridayOptStackB/FridayOptBadgeB
@onready var friday_opt_body_a: RichTextLabel = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayOptionsRow/FridayOptPanelA/FridayOptStackA/FridayOptBodyA
@onready var friday_opt_body_b: RichTextLabel = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayOptionsRow/FridayOptPanelB/FridayOptStackB/FridayOptBodyB
@onready var friday_choose_a: Button = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayOptionsRow/FridayOptPanelA/FridayOptStackA/FridayChooseA
@onready var friday_choose_b: Button = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayOptionsRow/FridayOptPanelB/FridayOptStackB/FridayChooseB
@onready var friday_opt_panel_a: PanelContainer = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayOptionsRow/FridayOptPanelA
@onready var friday_opt_panel_b: PanelContainer = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayOptionsRow/FridayOptPanelB
@onready var friday_next_button: Button = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/FridayIntroPage/FridayNextButton
@onready var week_summary_main_page: Control = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage
@onready var week_summary_title: Label = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/WeekHeroRow/WeekTitleStack/Title
@onready var week_summary_subtitle: Label = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/WeekHeroRow/WeekTitleStack/Subtitle
@onready var week_summary_body_scroll: ScrollContainer = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll
@onready var week_summary_left_heading: Label = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryLeft/SummaryLeftHeading
@onready var week_summary_progress: RichTextLabel = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryLeft/SummaryLeftBody/Progress
@onready var week_summary_tasks: RichTextLabel = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryLeft/SummaryLeftBody/Tasks
@onready var week_summary_deltas: RichTextLabel = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryLeft/SummaryLeftBody/Deltas
@onready var week_summary_hints: RichTextLabel = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryLeft/SummaryLeftBody/Hints
@onready var week_summary_col_sep: Control = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/ColSep
@onready var week_summary_right_column: Control = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryRight
@onready var week_summary_event_section: Label = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryRight/EventSectionTitle
@onready var week_summary_event_cast: HBoxContainer = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryRight/EventCastRow
@onready var week_summary_event_avatar: TextureRect = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryRight/EventCastRow/EventAvatar
@onready var week_summary_event_type: Label = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryRight/EventCastRow/EventTypeLabel
@onready var week_summary_event_speaker: Label = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryRight/EventSpeaker
@onready var week_summary_event_body: RichTextLabel = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryRight/EventBody
@onready var week_summary_event_buttons: HBoxContainer = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryRight/EventButtons
@onready var week_summary_choose_a: Button = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryRight/EventButtons/ChooseA
@onready var week_summary_choose_b: Button = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/WeekSummaryMainPage/SummaryScroll/SummaryColumns/SummaryRight/EventButtons/ChooseB
@onready var week_summary_continue: Button = $WeekSummaryOverlay/Wrap/DialogCenter/Dialog/Box/ContinueBtn
@onready var game_over_overlay: Control = $GameOverOverlay
@onready var game_over_dialog: PanelContainer = $GameOverOverlay/GameOverWrap/GameOverCenter/GameOverDialog
@onready var game_over_title: Label = $GameOverOverlay/GameOverWrap/GameOverCenter/GameOverDialog/GameOverBox/GameOverTitle
@onready var game_over_art: TextureRect = $GameOverOverlay/GameOverWrap/GameOverCenter/GameOverDialog/GameOverBox/GameOverHero/GameOverPortraitCol/GameOverArt
@onready var game_over_name: Label = $GameOverOverlay/GameOverWrap/GameOverCenter/GameOverDialog/GameOverBox/GameOverHero/GameOverPortraitCol/GameOverName
@onready var game_over_reason: RichTextLabel = $GameOverOverlay/GameOverWrap/GameOverCenter/GameOverDialog/GameOverBox/GameOverHero/GameOverTextCol/GameOverReason
@onready var game_over_advice_heading: Label = $GameOverOverlay/GameOverWrap/GameOverCenter/GameOverDialog/GameOverBox/GameOverHero/GameOverTextCol/GameOverAdviceHeading
@onready var game_over_advice: RichTextLabel = $GameOverOverlay/GameOverWrap/GameOverCenter/GameOverDialog/GameOverBox/GameOverHero/GameOverTextCol/GameOverAdvice
@onready var game_over_hint: Label = $GameOverOverlay/GameOverWrap/GameOverCenter/GameOverDialog/GameOverBox/GameOverHint
@onready var game_over_save_log: Button = $GameOverOverlay/GameOverWrap/GameOverCenter/GameOverDialog/GameOverBox/GameOverButtonRow/GameOverSaveLog
@onready var game_over_play_again: Button = $GameOverOverlay/GameOverWrap/GameOverCenter/GameOverDialog/GameOverBox/GameOverButtonRow/GameOverPlayAgain

var state := GameState.new()
var weekly_tasks: Array[Dictionary] = []
var current_event: Dictionary = {}
var selected_tasks: Array[Dictionary] = []
var waiting_event_choice := false
var event_prepared := false
var planning_locked := false

var card_by_index: Dictionary = {}
var slot_cards: Array = [null, null, null]
var sprint_slots: Array[SprintSlot] = []
var session_history: Array[String] = []
var status_note: String = ""
var event_avatar_investor_tex: Texture2D
var event_avatar_mamkin_tex: Texture2D
var event_avatar_cto_tex: Texture2D = preload("res://assets/cto.svg")
var event_avatar_marketer_tex: Texture2D = preload("res://assets/marketer.svg")
var event_avatar_cfo_tex: Texture2D = preload("res://assets/cfo.svg")
var event_avatar_hr_tex: Texture2D = preload("res://assets/hr.svg")
var event_avatar_coo_tex: Texture2D = preload("res://assets/coo.svg")
var event_avatar_cco_tex: Texture2D = preload("res://assets/cco.svg")
var stage_intro_seen: Dictionary = {}
## Первый экран «кто вы» показывается один раз за сессию перед интро этапа Build.
var game_welcome_seen: bool = false
var _stage_intro_close_key: String = ""
var _welcome_button_cursor_visible: bool = true
var _stage_intro_preview_float_tween: Tween
var _stage_start_button_dot_idx: int = 0
const WELCOME_START_BUTTON_TEXT_ON := "> Запустить_стартап.exe"
const WELCOME_START_BUTTON_TEXT_OFF := "> Запуститьстартап.exe"
const STAGE_START_BUTTON_TEXT_STATES := ["Приступить", "Приступить.", "Приступить..", "Приступить..."]
var pulse_compact_mode := true
## Снимок метрик на старте закрытия недели (до задач).
var _ws_snap_before: Dictionary = {}
var _ws_closed_week: int = 0
var _ws_reviews: Array[String] = []
var _ws_task_logs: Array[String] = []
var _ws_team_bonus_log: String = ""
var _ws_need_event_pick: bool = false
## В текущем окне итогов недели заложен реальный нежданчик (false = неделя 1, только обучение).
var _ws_summary_had_event: bool = false
## Портреты ролей для большого кадра «Пятничный нежданчик» (ключ = texture_key спикера).
var _friday_role_portraits: Dictionary = {}
var _friday_role_portraits_loaded: bool = false
const POS_COLOR := "#7CFF9A"
const NEG_COLOR := "#FF7A7A"
const NEU_COLOR := "#AEBAD0"
## Панель ачивок: размер плитки на экране (текстуры подгружаем `*_64.png`).
const ACHIEVEMENT_ICON_DISPLAY_SIZE := Vector2(48, 48)

const FRIDAY_MECHANIC_INTRO_BB := (
	"[center]В конце недели происходит событие. Тебе нужно выбрать один из двух вариантов. "
	+ "События зависят от твоих решений за неделю и могут как повлиять в лучшую сторону на стартап, "
	+ "так и сильно [i]ухудшить[/i] его позиции. Такие события называются «Пятничный нежданчик».[/center]"
)


func _friday_quiet_week_intro_bbcode(closed_week: int) -> String:
	## Неделя 1: обучение. Дальше — если `pick_event()` вернул пусто: не дублируем туториал.
	if closed_week <= 1:
		return FRIDAY_MECHANIC_INTRO_BB
	return (
		(
			"[center][color=%s]Пятничное событие на эту неделю не подобралось: под ваш этап и текущее состояние "
			+ "(команда, продукт, флаги вроде Startup Fellows / инвестконтакта) не осталось подходящего сценария в пуле. "
			+ "Засчитаны только задачи спринта и обычный такт кассы. Это не баг — иногда неделя проходит без нежданчика.[/color][/center]"
		)
		% NEU_COLOR
	)

## Базовая палитра UI; панели и акценты переключаются с этапом (`_assign_ui_theme_for_stage`).
const UI_BG_VOID := Color(0.09, 0.09, 0.11)
const UI_TEXT := Color(0.94, 0.94, 0.96)
const UI_TEXT_MUTED := Color(0.62, 0.62, 0.68)
var UI_PANEL_SPRINT := Color(0.12, 0.12, 0.14)
var UI_PANEL_CENTER := Color(0.10, 0.10, 0.12)
var UI_PANEL_EVENT := Color(0.13, 0.11, 0.12)
var UI_PANEL_HEADER := Color(0.14, 0.12, 0.11)
var UI_BORDER := Color(0.04, 0.04, 0.05)
var UI_BORDER_ACCENT := Color(1.0, 0.42, 0.08)
var UI_TEXT_ACCENT := Color(1.0, 0.88, 0.35)
var UI_BTN_PRIMARY_BG := Color(0.18, 0.16, 0.15)
var UI_BTN_PRIMARY_BG_H := Color(0.26, 0.22, 0.18)
var UI_BTN_CTA_BG := Color(0.22, 0.12, 0.06)
var UI_BTN_CTA_BG_H := Color(0.32, 0.16, 0.06)
var UI_BTN_CTA_BORDER := Color(1.0, 0.82, 0.12)
var UI_SLOT_EMPTY := Color(0.08, 0.10, 0.14)
var UI_SLOT_BORDER := Color(0.08, 0.09, 0.12)
## Заливка карточек задач (рамка — через UI_BORDER_ACCENT).
var UI_CARD_BASE := Color(0.13, 0.17, 0.24)
var _ui_palette_stage_key: String = ""
## Лочим визуальный этап (фон/тема/BGM) на время окна «Итог недели».
var _stage_presentation_lock_key: String = ""

## Прозрачность заливок HUD: фон этапа (Background) просвечивает между панелями и сквозь них.
const UI_GLASS_MAIN := 0.78
const UI_GLASS_HEADER := 0.84
const UI_GLASS_EVENT := 0.80
const UI_GLASS_SLOT := 0.68
const UI_GLASS_CARD := 0.74
const UI_GLASS_RISK := 0.82

const GAME_OVER_WON_MP3 := "res://assets/music/mamkin_game_over_won.mp3"
const GAME_OVER_LOST_MP3 := "res://assets/music/mamkin_game_over_lost.mp3"

const BGM_BUILD_PATH := "res://assets/music/pixel_profit.mp3"
const BGM_FIT_PATH := "res://assets/music/mamkin_stage_fit.mp3"
const BGM_SCALE_PATH := "res://assets/music/mamkin_stage_scale.mp3"

var _game_over_won_stream: AudioStream
var _game_over_lost_stream: AudioStream
var _bgm_stream_by_path: Dictionary = {}
var _bgm_applied_stage_key: String = ""

const GAME_OVER_ART := {
	"bankrupt": "res://assets/mamkin_game_over_bankrupt.png",
	"scold": "res://assets/mamkin_game_over_bankrupt.png",
	"dilution": "res://assets/mamkin_game_over_burnout.png",
	"burnout": "res://assets/mamkin_game_over_burnout.png",
	"struggle": "res://assets/mamkin_game_over_burnout.png",
	"triumph": "res://assets/mamkin_game_over_triumph.png",
}

## Текстуры этапов подгружаются в рантайме (не preload): иначе парсер падает, пока Godot не импортировал PNG.
var _stage_textures: Dictionary = {}
## Превью для модального окна начала этапа (слева в диалоге).
var _stage_modal_preview_textures: Dictionary = {}
var _welcome_intro_bg_texture: Texture2D

func _bgm_path_for_stage_key(stage_key: String) -> String:
	match stage_key:
		"fit":
			return BGM_FIT_PATH
		"scale":
			return BGM_SCALE_PATH
		_:
			return BGM_BUILD_PATH


func _normalized_stage_key(raw_stage_key: String) -> String:
	if raw_stage_key != "fit" and raw_stage_key != "scale":
		return "build"
	return raw_stage_key


func _effective_stage_key() -> String:
	if _stage_presentation_lock_key != "":
		return _stage_presentation_lock_key
	return _normalized_stage_key(str(state.get_stage_info().get("key", "build")))


func _load_bgm_stream(path: String) -> AudioStream:
	if _bgm_stream_by_path.has(path):
		return _bgm_stream_by_path[path] as AudioStream
	if not ResourceLoader.exists(path):
		return null
	var st: AudioStream = load(path) as AudioStream
	_bgm_stream_by_path[path] = st
	return st


## Фоновая музыка по этапу (Build / Fit / Scale). Не трогаем во время game over или проигрыша финального трека.
func _apply_stage_background_music() -> void:
	if state.game_over:
		return
	if game_over_music.playing:
		return
	var stage_key: String = _effective_stage_key()
	if stage_key == _bgm_applied_stage_key and bg_music.playing:
		return
	var path: String = _bgm_path_for_stage_key(stage_key)
	var stream: AudioStream = _load_bgm_stream(path)
	if stream == null:
		return
	_bgm_applied_stage_key = stage_key
	bg_music.stop()
	bg_music.stream = stream
	if stream is AudioStreamMP3:
		(stream as AudioStreamMP3).loop = true
	bg_music.play()


func _ensure_game_over_music_streams() -> void:
	if _game_over_won_stream != null and _game_over_lost_stream != null:
		return
	if ResourceLoader.exists(GAME_OVER_WON_MP3):
		_game_over_won_stream = load(GAME_OVER_WON_MP3) as AudioStream
	if ResourceLoader.exists(GAME_OVER_LOST_MP3):
		_game_over_lost_stream = load(GAME_OVER_LOST_MP3) as AudioStream


func _play_game_over_music_for_art_key(art_key: String) -> void:
	_ensure_game_over_music_streams()
	if bg_music.playing:
		bg_music.stop()
	game_over_music.stop()
	var stream: AudioStream = _game_over_won_stream if art_key == "triumph" else _game_over_lost_stream
	if stream == null:
		return
	game_over_music.stream = stream
	if stream is AudioStreamMP3:
		(stream as AudioStreamMP3).loop = false
	game_over_music.play()

func _ready() -> void:
	_ensure_stage_textures_loaded()
	sprint_slots = [slot0, slot1, slot2]
	backlog_row.set_main(self)
	for s in sprint_slots:
		s.set_main(self)
	_apply_stage_ui_theme()
	_apply_log_toggle_icon()
	event_panel.visible = false
	subtitle_label.text = "Спринт → итоги недели во всплывающем окне. Пятничный нежданчик — там же. Лог — справа внизу."
	event_avatar_investor_tex = event_avatar.texture
	event_avatar_mamkin_tex = header_avatar.texture
	next_week_button.pressed.connect(_on_next_week_pressed)
	launch_now_button.pressed.connect(_on_launch_now_pressed)
	log_toggle_button.pressed.connect(_on_log_toggle_pressed)
	log_overlay_save_button.pressed.connect(_on_log_save_pressed)
	log_overlay_close_button.pressed.connect(_on_log_close_pressed)
	pulse_mode_toggle_button.pressed.connect(_on_pulse_mode_toggle_pressed)
	week_summary_choose_a.pressed.connect(func() -> void: _on_week_summary_event_pick(0))
	week_summary_choose_b.pressed.connect(func() -> void: _on_week_summary_event_pick(1))
	friday_choose_a.pressed.connect(func() -> void: _on_week_summary_event_pick(0))
	friday_choose_b.pressed.connect(func() -> void: _on_week_summary_event_pick(1))
	friday_next_button.pressed.connect(_on_friday_intro_next_pressed)
	week_summary_continue.pressed.connect(_on_week_summary_continue_pressed)
	game_over_play_again.pressed.connect(_on_game_over_play_again_pressed)
	game_over_save_log.pressed.connect(_on_game_over_save_log_pressed)
	stage_intro_start_button.pressed.connect(_on_stage_intro_start_pressed)
	welcome_next_button.pressed.connect(_on_welcome_intro_next_pressed)
	_configure_welcome_body_scroll()
	_start_welcome_intro_fx()
	_start_stage_start_button_text_fx()
	log_scroll.visible = false
	_update_pulse_mode_button_text()
	_apply_pulse_mode_layout()
	prepare_week()
	_refresh_log_panel_text()


func _apply_log_toggle_icon() -> void:
	var icon_path := "res://assets/icons/icon_log_32.png"
	if not ResourceLoader.exists(icon_path):
		return
	var tex := load(icon_path) as Texture2D
	if tex == null:
		return
	log_toggle_button.icon = tex
	log_toggle_button.text = "Лог"


func _configure_welcome_body_scroll() -> void:
	## Текст с переносом по ширине колонки; прокрутка только по вертикали.
	welcome_body.autowrap_mode = TextServer.AUTOWRAP_WORD
	welcome_body_scroll.horizontal_scroll_mode = ScrollContainer.SCROLL_MODE_DISABLED
	welcome_body_scroll.vertical_scroll_mode = ScrollContainer.SCROLL_MODE_AUTO


## Подгоняет ширину Label под область ScrollContainer (только X). Высоту не фиксируем — иначе легко получить 0px и «схлопнуть» весь блок.
func _refresh_welcome_body_scroll_layout() -> void:
	if welcome_body == null or welcome_body_scroll == null:
		return
	var w: float = welcome_body_scroll.size.x
	if w < 2.0:
		return
	welcome_body.size_flags_horizontal = Control.SIZE_EXPAND_FILL
	welcome_body.size_flags_vertical = Control.SIZE_SHRINK_BEGIN
	welcome_body.custom_minimum_size = Vector2(w, 0.0)
	welcome_body_scroll.scroll_horizontal = 0
	welcome_body_scroll.scroll_vertical = 0


func _refresh_welcome_body_scroll_after_text() -> void:
	_refresh_welcome_body_scroll_layout()
	if welcome_body_scroll != null and welcome_body_scroll.size.x < 2.0:
		var t: SceneTreeTimer = get_tree().create_timer(0.05)
		t.timeout.connect(_refresh_welcome_body_scroll_layout, CONNECT_ONE_SHOT)


func _start_welcome_intro_fx() -> void:
	var base_y: float = welcome_mascot.position.y
	var float_tween: Tween = create_tween().set_loops()
	float_tween.tween_property(welcome_mascot, "position:y", base_y - 10.0, 1.2).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_IN_OUT)
	float_tween.tween_property(welcome_mascot, "position:y", base_y, 1.2).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_IN_OUT)
	welcome_next_button.text = WELCOME_START_BUTTON_TEXT_ON
	var blink_tween: Tween = create_tween().set_loops()
	blink_tween.tween_interval(0.5)
	blink_tween.tween_callback(Callable(self, "_toggle_welcome_button_cursor"))


func _start_stage_intro_preview_fx() -> void:
	if _stage_intro_preview_float_tween != null and _stage_intro_preview_float_tween.is_valid():
		_stage_intro_preview_float_tween.kill()
	var base_y: float = stage_intro_preview.position.y
	_stage_intro_preview_float_tween = create_tween().set_loops()
	_stage_intro_preview_float_tween.tween_property(stage_intro_preview, "position:y", base_y - 10.0, 1.25).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_IN_OUT)
	_stage_intro_preview_float_tween.tween_property(stage_intro_preview, "position:y", base_y, 1.25).set_trans(Tween.TRANS_SINE).set_ease(Tween.EASE_IN_OUT)


func _start_stage_start_button_text_fx() -> void:
	_stage_start_button_dot_idx = 0
	stage_intro_start_button.text = STAGE_START_BUTTON_TEXT_STATES[_stage_start_button_dot_idx]
	var t: Tween = create_tween().set_loops()
	t.tween_interval(0.5)
	t.tween_callback(Callable(self, "_tick_stage_start_button_text_fx"))


func _tick_stage_start_button_text_fx() -> void:
	_stage_start_button_dot_idx = (_stage_start_button_dot_idx + 1) % STAGE_START_BUTTON_TEXT_STATES.size()
	stage_intro_start_button.text = STAGE_START_BUTTON_TEXT_STATES[_stage_start_button_dot_idx]


func _toggle_welcome_button_cursor() -> void:
	_welcome_button_cursor_visible = not _welcome_button_cursor_visible
	welcome_next_button.text = WELCOME_START_BUTTON_TEXT_ON if _welcome_button_cursor_visible else WELCOME_START_BUTTON_TEXT_OFF

func _on_log_toggle_pressed() -> void:
	log_overlay.visible = true
	log_toggle_button.text = "📘 Лог открыт"
	log_overlay_body.text = log_label.text
	log_overlay_scroll.scroll_vertical = 0
	_refresh_log_panel_text()

func _on_log_close_pressed() -> void:
	log_overlay.visible = false
	log_toggle_button.text = "📘 Лог"

## Сохраняет текущий текст лога в Downloads или user://logs. Возвращает путь для UI или "" при ошибке.
func _save_session_log_to_file() -> String:
	_refresh_log_panel_text()
	var stamp: String = Time.get_datetime_string_from_system(false, true).replace(":", "-").replace(" ", "_")
	var base_name := "mamkin_unicorn_log_%s.txt" % stamp
	var content: String = log_label.text
	var saved_display: String = ""
	var downloads: String = OS.get_system_dir(OS.SYSTEM_DIR_DOWNLOADS)
	if not downloads.is_empty():
		var dl_path: String = downloads.path_join(base_name)
		var f_dl: FileAccess = FileAccess.open(dl_path, FileAccess.WRITE)
		if f_dl != null:
			f_dl.store_string(content)
			f_dl.close()
			saved_display = dl_path
	if saved_display.is_empty():
		var user_dir := "user://logs"
		DirAccess.make_dir_recursive_absolute(user_dir)
		var user_path := "%s/%s" % [user_dir, base_name]
		var f_user: FileAccess = FileAccess.open(user_path, FileAccess.WRITE)
		if f_user == null:
			return ""
		f_user.store_string(content)
		f_user.close()
		saved_display = ProjectSettings.globalize_path(user_path)
	return saved_display


func _on_log_save_pressed() -> void:
	var saved_display: String = _save_session_log_to_file()
	if saved_display.is_empty():
		_set_status_note("Не удалось сохранить лог. Проверь права на запись.")
		log_overlay_hint.text = "Ошибка: не удалось записать ни в Загрузки, ни в каталог данных игры."
		return
	var dl: String = OS.get_system_dir(OS.SYSTEM_DIR_DOWNLOADS)
	if dl != "" and saved_display.begins_with(dl):
		_set_status_note("Лог сохранён в «Загрузки»: %s" % saved_display)
		log_overlay_hint.text = "Файл в папке «Загрузки» (Downloads): %s" % saved_display
	else:
		_set_status_note("Папка «Загрузки» недоступна — лог сохранён: %s" % saved_display)
		log_overlay_hint.text = "Сохранено (запасной путь): %s" % saved_display


func _on_game_over_save_log_pressed() -> void:
	var saved_display: String = _save_session_log_to_file()
	if saved_display.is_empty():
		game_over_hint.text = "Не удалось сохранить лог. Проверь права на запись или открой «📘 Лог» и попробуй снова."
		_set_status_note("Не удалось сохранить лог с экрана конца игры.")
		return
	var dl: String = OS.get_system_dir(OS.SYSTEM_DIR_DOWNLOADS)
	if dl != "" and saved_display.begins_with(dl):
		game_over_hint.text = "Лог сохранён в «Загрузки»:\n%s" % saved_display
		_set_status_note("Лог сохранён: %s" % saved_display)
	else:
		game_over_hint.text = "Лог сохранён (запасной путь):\n%s" % saved_display
		_set_status_note("Лог сохранён: %s" % saved_display)

func _on_pulse_mode_toggle_pressed() -> void:
	pulse_compact_mode = not pulse_compact_mode
	_update_pulse_mode_button_text()
	_apply_pulse_mode_layout()
	_update_dashboard()

func _update_pulse_mode_button_text() -> void:
	pulse_mode_toggle_button.text = "Режим: кратко" if pulse_compact_mode else "Режим: подробно"

func _apply_pulse_mode_layout() -> void:
	var is_detailed: bool = not pulse_compact_mode
	metrics_left.scroll_active = is_detailed
	metrics_right.scroll_active = is_detailed
	if is_detailed:
		metrics_left.scroll_to_line(0)
		metrics_right.scroll_to_line(0)

func _set_status_note(text: String) -> void:
	status_note = text
	_refresh_log_panel_text()

func _refresh_log_panel_text() -> void:
	var lines: Array[String] = []
	lines.append("[Текущий статус]")
	lines.append(status_note if status_note != "" else "Нет активной заметки.")
	lines.append("")
	lines.append("[История сессии]")
	if session_history.is_empty():
		lines.append("Пока пусто. После закрытия недели здесь появятся решения и пояснения.")
	else:
		for i in session_history.size():
			lines.append(session_history[i])
			if i < session_history.size() - 1:
				lines.append("")
	log_label.text = "\n".join(lines)
	log_overlay_body.text = log_label.text

func _stage_intro_content(stage_key: String) -> Dictionary:
	match stage_key:
		"build":
			return {
				"badge": "",
				"title": "🚀 ЭТАП 1: BUILD",
				"goal": "Найти реальную боль аудитории и собрать работающий MVP (минимально жизнеспособный продукт). Ваша главная цель на старте — не сжечь все деньги (runway) до релиза первой версии.",
				"welcome_title": "Вы — Мамкин. И вы построите «Единорога»!",
				"welcome_body": (
					"Студенческая мечта стала реальностью. Вы вложили последние кровные в свой стартап, чтобы перевернуть рынок и войти в историю. Ваша главная цель — вырастить «Единорога» (так в IT называют стартапы, чья оценка превысила 1 миллиард долларов).\n\n"
					+ "Но путь на вершину жесток. Вам предстоит пройти три ключевых этапа:\n"
					+ "• Build — создать работающий продукт.\n"
					+ "• Fit — доказать, что продукт нужен рынку.\n"
					+ "• Scale — масштабироваться и захватить индустрию.\n\n"
					+ "Ваш главный враг — время, а деньги стремительно сгорают. Каждую неделю вам предложат 5 возможных действий: выбрать можно только 3. Каждое решение имеет цену и последствия.\n\n"
					+ "Доберитесь до миллиардной оценки раньше, чем наступит кассовый разрыв. Удачи, фаундер!"
				),
				"intro": "Код, гипотезы, бессонные ночи. Каждую неделю вам нужно выбрать ровно 3 задачи из 5 предложенных. Именно этот фокус решит, доживёте ли вы до следующего этапа или закроетесь в мамином гараже.",
				"panel_bg": Color(0.11, 0.15, 0.25),
				"panel_border": Color(0.34, 0.62, 1.0),
				"accent": Color(0.62, 0.80, 1.0)
			}
		"fit":
			return {
				"badge": "",
				"title": "🎯 ЭТАП 2: FIT (Product-Market Fit)",
				"goal": "Найти свой PMF (Product-Market Fit). Доказать, что ваш продукт реально хотят покупать, собрать первые твёрдые метрики и не умереть в стартаперской «долине смерти».",
				"intro": "Правила те же: выбирайте 3 действия из 5 каждую неделю. Но ставки растут. Вам предстоит слушать пользователей, пивотить (менять) продукт на ходу и готовиться к первым серьёзным продажам.",
				"panel_bg": Color(0.20, 0.12, 0.24),
				"panel_border": Color(0.84, 0.44, 1.0),
				"accent": Color(0.94, 0.70, 1.0)
			}
		_:
			return {
				"badge": "",
				"title": "🦄 ЭТАП 3: SCALE",
				"goal": "Педаль газа в пол! Ваша цель — масштабировать маркетинг, не сломать юнит-экономику при бурном росте и довести оценку компании до заветного $1 млрд.",
				"intro": "Каждую неделю выбирайте 3 стратегических шага из 5. Управляйте растущим хаосом, привлекайте новые раунды инвестиций и захватите этот рынок, став настоящим «Единорогом»!",
				"panel_bg": Color(0.24, 0.14, 0.09),
				"panel_border": Color(1.0, 0.65, 0.20),
				"accent": Color(1.0, 0.86, 0.42)
			}

func _maybe_show_stage_intro() -> void:
	var stage_key: String = str(state.get_stage_info().get("key", "build"))
	if stage_intro_seen.get(stage_key, false):
		return
	if stage_key == "build" and not game_welcome_seen:
		_show_player_welcome_intro()
		return
	_show_stage_intro(stage_key)


func _ensure_stage_textures_loaded() -> void:
	if not _stage_textures.is_empty():
		return
	var fallback: Texture2D = load("res://assets/pixel_pattern.png") as Texture2D
	for key in ["build", "fit", "scale"]:
		var path: String = "res://assets/%s.png" % key
		var tex: Texture2D = load(path) as Texture2D
		_stage_textures[key] = tex if tex != null else fallback


func _texture_for_stage_key(k: String) -> Texture2D:
	_ensure_stage_textures_loaded()
	var fb: Texture2D = _stage_textures.get("build") as Texture2D
	match k:
		"fit":
			return _stage_textures.get("fit", fb) as Texture2D
		"scale":
			return _stage_textures.get("scale", fb) as Texture2D
		_:
			return fb


func _texture_stage_intro_modal_preview(stage_key: String) -> Texture2D:
	if _stage_modal_preview_textures.has(stage_key):
		return _stage_modal_preview_textures[stage_key] as Texture2D
	var sk: String = stage_key
	if sk != "fit" and sk != "scale":
		sk = "build"
	var path: String = "res://assets/stage_intro_preview_%s.png" % sk
	var tex: Texture2D = load(path) as Texture2D
	if tex == null:
		tex = load("res://assets/stage_intro_preview_build.png") as Texture2D
	_stage_modal_preview_textures[stage_key] = tex
	return tex


func _texture_welcome_intro_background() -> Texture2D:
	## Фон welcome: `res://assets/welcome_bg.png` (можно заменить своей картинкой, имя и путь те же).
	if _welcome_intro_bg_texture != null:
		return _welcome_intro_bg_texture
	const WELCOME_BG_PATH := "res://assets/welcome_bg.png"
	if ResourceLoader.exists(WELCOME_BG_PATH):
		var tx: Texture2D = load(WELCOME_BG_PATH) as Texture2D
		if tx != null:
			_welcome_intro_bg_texture = tx
			return tx
	_welcome_intro_bg_texture = _texture_for_stage_key("build")
	return _welcome_intro_bg_texture


func _ui_stage_key_normalized() -> String:
	return _effective_stage_key()


func _assign_ui_theme_for_stage(sk: String) -> void:
	match sk:
		"fit":
			## Фиолетовый тон под fit.png и интро Fit.
			UI_PANEL_SPRINT = Color(0.13, 0.09, 0.15)
			UI_PANEL_CENTER = Color(0.11, 0.08, 0.13)
			UI_PANEL_EVENT = Color(0.14, 0.10, 0.15)
			UI_PANEL_HEADER = Color(0.12, 0.09, 0.14)
			UI_BORDER = Color(0.06, 0.04, 0.07)
			UI_BORDER_ACCENT = Color(0.78, 0.42, 0.95)
			UI_TEXT_ACCENT = Color(0.94, 0.72, 1.0)
			UI_BTN_PRIMARY_BG = Color(0.16, 0.11, 0.19)
			UI_BTN_PRIMARY_BG_H = Color(0.22, 0.15, 0.26)
			UI_BTN_CTA_BG = Color(0.20, 0.10, 0.22)
			UI_BTN_CTA_BG_H = Color(0.28, 0.14, 0.30)
			UI_BTN_CTA_BORDER = Color(0.92, 0.55, 1.0)
			UI_SLOT_EMPTY = Color(0.09, 0.07, 0.12)
			UI_SLOT_BORDER = Color(0.14, 0.10, 0.18)
			UI_CARD_BASE = Color(0.14, 0.10, 0.20)
		"scale":
			## Тёплый янтарь под scale.png и интро Scale.
			UI_PANEL_SPRINT = Color(0.15, 0.10, 0.08)
			UI_PANEL_CENTER = Color(0.13, 0.09, 0.07)
			UI_PANEL_EVENT = Color(0.16, 0.11, 0.09)
			UI_PANEL_HEADER = Color(0.14, 0.10, 0.08)
			UI_BORDER = Color(0.07, 0.05, 0.04)
			UI_BORDER_ACCENT = Color(1.0, 0.55, 0.18)
			UI_TEXT_ACCENT = Color(1.0, 0.86, 0.42)
			UI_BTN_PRIMARY_BG = Color(0.19, 0.12, 0.09)
			UI_BTN_PRIMARY_BG_H = Color(0.26, 0.16, 0.11)
			UI_BTN_CTA_BG = Color(0.26, 0.12, 0.06)
			UI_BTN_CTA_BG_H = Color(0.34, 0.16, 0.08)
			UI_BTN_CTA_BORDER = Color(1.0, 0.72, 0.22)
			UI_SLOT_EMPTY = Color(0.11, 0.08, 0.06)
			UI_SLOT_BORDER = Color(0.16, 0.11, 0.08)
			UI_CARD_BASE = Color(0.18, 0.12, 0.09)
		_:
			## Build: холодный синий к build.png / «цех».
			UI_PANEL_SPRINT = Color(0.10, 0.12, 0.17)
			UI_PANEL_CENTER = Color(0.08, 0.10, 0.15)
			UI_PANEL_EVENT = Color(0.11, 0.11, 0.16)
			UI_PANEL_HEADER = Color(0.10, 0.11, 0.16)
			UI_BORDER = Color(0.04, 0.05, 0.07)
			UI_BORDER_ACCENT = Color(0.38, 0.58, 0.96)
			UI_TEXT_ACCENT = Color(0.62, 0.80, 1.0)
			UI_BTN_PRIMARY_BG = Color(0.12, 0.14, 0.19)
			UI_BTN_PRIMARY_BG_H = Color(0.18, 0.22, 0.30)
			UI_BTN_CTA_BG = Color(0.10, 0.16, 0.26)
			UI_BTN_CTA_BG_H = Color(0.14, 0.22, 0.34)
			UI_BTN_CTA_BORDER = Color(0.45, 0.62, 0.98)
			UI_SLOT_EMPTY = Color(0.07, 0.09, 0.14)
			UI_SLOT_BORDER = Color(0.10, 0.14, 0.20)
			UI_CARD_BASE = Color(0.10, 0.14, 0.22)


func _reapply_stage_dependent_ui_styles() -> void:
	_apply_panel_styles()
	_style_sprint_slots()
	_style_buttons()
	_style_typography()
	_style_risk_badge()
	_style_event_speaker_chip()
	_style_stage_intro()
	_style_log_overlay()
	_style_week_summary_overlay()
	_style_game_over_overlay()
	_style_scrollbars()
	for _idx in card_by_index:
		_style_task_card(card_by_index[_idx])
	_update_risk_visuals()


func _apply_stage_ui_theme() -> void:
	var sk: String = _ui_stage_key_normalized()
	if sk == _ui_palette_stage_key:
		return
	_ui_palette_stage_key = sk
	_assign_ui_theme_for_stage(sk)
	_reapply_stage_dependent_ui_styles()


func _apply_stage_background() -> void:
	var k: String = _effective_stage_key()
	stage_bg_rect.texture = _texture_for_stage_key(k)
	match k:
		"fit":
			stage_bg_rect.modulate = Color(0.90, 0.86, 0.96, 1.0)
		"scale":
			stage_bg_rect.modulate = Color(0.96, 0.88, 0.78, 1.0)
		_:
			stage_bg_rect.modulate = Color(0.82, 0.84, 0.88, 1.0)


func _apply_stage_intro_panel_style(c: Dictionary) -> void:
	var panel_bg: Color = c["panel_bg"] as Color
	var panel_border: Color = c["panel_border"] as Color
	var dlg := StyleBoxFlat.new()
	dlg.bg_color = _glass(panel_bg, 0.54)
	dlg.border_color = _glass(panel_border, 0.62)
	dlg.border_width_left = 1
	dlg.border_width_top = 1
	dlg.border_width_right = 1
	dlg.border_width_bottom = 1
	dlg.corner_radius_top_left = 20
	dlg.corner_radius_top_right = 20
	dlg.corner_radius_bottom_right = 20
	dlg.corner_radius_bottom_left = 20
	dlg.content_margin_left = 18
	dlg.content_margin_top = 18
	dlg.content_margin_right = 18
	dlg.content_margin_bottom = 18
	stage_intro_dialog.add_theme_stylebox_override("panel", dlg)
	var text_panel := StyleBoxFlat.new()
	text_panel.bg_color = _glass(Color(0.02, 0.03, 0.06, 1.0), 0.45)
	text_panel.border_color = _glass(panel_border.lightened(0.12), 0.42)
	text_panel.border_width_left = 1
	text_panel.border_width_top = 1
	text_panel.border_width_right = 1
	text_panel.border_width_bottom = 1
	text_panel.corner_radius_top_left = 14
	text_panel.corner_radius_top_right = 14
	text_panel.corner_radius_bottom_right = 14
	text_panel.corner_radius_bottom_left = 14
	text_panel.content_margin_left = 14
	text_panel.content_margin_top = 12
	text_panel.content_margin_right = 14
	text_panel.content_margin_bottom = 12
	stage_intro_text_panel.add_theme_stylebox_override("panel", text_panel)


func _apply_welcome_intro_panel_style(accent: Color) -> void:
	var sb := StyleBoxFlat.new()
	sb.bg_color = _glass(Color(0.03, 0.04, 0.08, 1.0), 0.52)
	sb.border_color = _glass(accent, 0.55)
	sb.border_width_left = 1
	sb.border_width_top = 1
	sb.border_width_right = 1
	sb.border_width_bottom = 1
	sb.corner_radius_top_left = 20
	sb.corner_radius_top_right = 20
	sb.corner_radius_bottom_right = 20
	sb.corner_radius_bottom_left = 20
	stage_intro_dialog.add_theme_stylebox_override("panel", sb)


func _stage_pill_display_name(key: String) -> String:
	match key:
		"build":
			return "Build"
		"fit":
			return "Fit"
		_:
			return "Scale"


## Строка «Build → Fit → Scale»: текущий этап — крупнее и CAPS, остальные — мельче.
func _update_stage_intro_dialog_dimensions(is_welcome: bool = false) -> void:
	## Ширина — по окну. Первое окно (приветствие): ограниченная высота. Второе (этап): высота по контенту, без пустоты.
	var vp: Vector2 = get_viewport().get_visible_rect().size
	var margin: float = 64.0
	var w: float = clampf(vp.x - margin * 2.0, 640.0, 1480.0)
	if is_welcome:
		var h: float = clampf(vp.y - margin * 2.0, 520.0, 920.0)
		## Крупный спрайт слева: чуть шире дефолта, но не шире окна (иначе вылезает за край).
		var ww: float = clampf(maxf(w, 800.0), 640.0, vp.x - margin * 2.0)
		stage_intro_dialog.custom_minimum_size = Vector2(ww, h)
	else:
		stage_intro_dialog.custom_minimum_size = Vector2(w, 0)


func _apply_stage_pill_row(current_key: String, accent: Color) -> void:
	const FONT_ACTIVE := 22
	const FONT_INACTIVE := 14
	var keys: Array[String] = ["build", "fit", "scale"]
	var pills: Array[Label] = [stage_pill_build, stage_pill_fit, stage_pill_scale]
	var sep_color: Color = UI_TEXT_MUTED.darkened(0.08)
	for i in 3:
		var k: String = keys[i]
		var pill: Label = pills[i]
		if k == current_key:
			pill.text = _stage_pill_display_name(k).to_upper()
			pill.add_theme_font_size_override("font_size", FONT_ACTIVE)
			pill.add_theme_color_override("font_color", accent)
		else:
			pill.text = _stage_pill_display_name(k)
			pill.add_theme_font_size_override("font_size", FONT_INACTIVE)
			pill.add_theme_color_override("font_color", UI_TEXT_MUTED)
	for sep in [stage_pill_sep1, stage_pill_sep2]:
		sep.add_theme_font_size_override("font_size", FONT_INACTIVE)
		sep.add_theme_color_override("font_color", sep_color)


func _show_player_welcome_intro() -> void:
	_stage_intro_close_key = "build"
	_ensure_stage_textures_loaded()
	stage_intro_art.texture = _texture_welcome_intro_background()
	var c: Dictionary = _stage_intro_content("build")
	var accent: Color = c["accent"] as Color
	_apply_welcome_intro_panel_style(accent)
	welcome_title.text = str(c.get("welcome_title", ""))
	welcome_body.text = str(c.get("welcome_body", ""))
	welcome_title.add_theme_color_override("font_color", accent)
	welcome_body.add_theme_color_override("font_color", UI_TEXT)
	welcome_mascot.texture = header_avatar.texture
	## Ширина колонки задаётся HBox (40% / 60%); не фиксируем квадрат — иначе ломается пропорция.
	welcome_mascot.custom_minimum_size = Vector2.ZERO
	stage_pill_row.visible = false
	welcome_intro_page.visible = true
	_update_stage_intro_dialog_dimensions(true)
	call_deferred("_refresh_welcome_body_scroll_after_text")
	stage_intro_page.visible = false
	stage_intro_corner_mascot.visible = false
	stage_intro_overlay.visible = true


func _on_welcome_intro_next_pressed() -> void:
	game_welcome_seen = true
	welcome_intro_page.visible = false
	stage_intro_page.visible = true
	_show_stage_intro("build")


func _show_stage_intro(stage_key: String) -> void:
	_stage_intro_close_key = stage_key
	stage_intro_art.texture = _texture_for_stage_key(stage_key)
	stage_intro_preview.texture = _texture_stage_intro_modal_preview(stage_key)
	var c: Dictionary = _stage_intro_content(stage_key)
	_apply_stage_intro_panel_style(c)
	var accent: Color = c["accent"] as Color
	var badge_txt: String = str(c.get("badge", "")).strip_edges()
	stage_intro_badge.visible = badge_txt != ""
	stage_intro_badge.text = badge_txt
	stage_intro_title.text = str(c["title"])
	stage_intro_goal.text = str(c["goal"])
	stage_intro_body.text = str(c["intro"])
	stage_intro_badge.add_theme_color_override("font_color", accent)
	stage_intro_title.add_theme_color_override("font_color", accent)
	stage_intro_goal.add_theme_color_override("font_color", UI_TEXT)
	stage_intro_body.add_theme_color_override("font_color", UI_TEXT_MUTED.lightened(0.2))
	_apply_stage_pill_row(stage_key, accent)
	stage_pill_row.visible = true
	welcome_intro_page.visible = false
	stage_intro_page.visible = true
	stage_intro_corner_mascot.visible = false
	stage_intro_overlay.visible = true
	_update_stage_intro_dialog_dimensions(false)
	call_deferred("_start_stage_intro_preview_fx")

func _on_stage_intro_start_pressed() -> void:
	if _stage_intro_close_key != "":
		stage_intro_seen[_stage_intro_close_key] = true
		_stage_intro_close_key = ""
	stage_intro_overlay.visible = false

func is_planning_locked() -> bool:
	return planning_locked

func can_accept_task_drop(data: Variant) -> bool:
	if planning_locked:
		return false
	return data is Dictionary and str(data.get("kind", "")) == "task"

func can_return_task_to_backlog(data: Variant) -> bool:
	if planning_locked:
		return false
	if not ( data is Dictionary ):
		return false
	return int(data.get("from_slot", -1)) >= 0

func on_task_drop_on_slot(to_slot: int, data: Variant) -> void:
	if not can_accept_task_drop(data):
		return
	var tidx: int = int(data["task_index"])
	var from_slot: int = int(data.get("from_slot", -1))
	var card: TaskCard = card_by_index.get(tidx, null)
	if card == null:
		return
	if from_slot == to_slot:
		return

	var target_card: TaskCard = slot_cards[to_slot]
	var has_from_slot: bool = from_slot >= 0 and from_slot < sprint_slots.size()
	if target_card != null and target_card != card and has_from_slot:
		# Swap двух занятых слотов:
		# сначала снимаем обе карточки, затем монтируем обратно.
		# Иначе mount_card() может queue_free() исходную карточку.
		if card.get_parent():
			card.get_parent().remove_child(card)
		if target_card.get_parent():
			target_card.get_parent().remove_child(target_card)
		sprint_slots[from_slot].mount_card(target_card)
		slot_cards[from_slot] = target_card
		sprint_slots[to_slot].mount_card(card)
		slot_cards[to_slot] = card
		_rebuild_selected_tasks_from_slots()
		_update_selected_text()
		return
	elif target_card != null and target_card != card:
		# Перетаскивание из бэклога в занятый слот => заменяем,
		# старую карточку возвращаем обратно в бэклог.
		if target_card.get_parent():
			target_card.get_parent().remove_child(target_card)
		target_card.set_slot_index(-1)
		backlog_row.add_child(target_card)
		slot_cards[to_slot] = null

	var par: Node = card.get_parent()
	if par:
		par.remove_child(card)
	if has_from_slot:
		# Если был swap, from_slot уже занят target_card и его не надо очищать.
		if slot_cards[from_slot] == null:
			sprint_slots[from_slot].show_empty()

	sprint_slots[to_slot].mount_card(card)
	slot_cards[to_slot] = card
	_rebuild_selected_tasks_from_slots()
	_update_selected_text()

func return_task_to_backlog(data: Variant) -> void:
	if not can_return_task_to_backlog(data):
		return
	var from_slot: int = int(data.get("from_slot", -1))
	var tidx: int = int(data["task_index"])
	var card: TaskCard = card_by_index.get(tidx, null)
	if card == null or from_slot < 0:
		return
	if card.get_parent() == sprint_slots[from_slot]:
		sprint_slots[from_slot].remove_child(card)
	sprint_slots[from_slot].show_empty()
	card.set_slot_index(-1)
	backlog_row.add_child(card)
	slot_cards[from_slot] = null
	_rebuild_selected_tasks_from_slots()
	_update_selected_text()

func prepare_week() -> void:
	week_summary_overlay.visible = false
	if state.game_over:
		_show_game_over()
		return
	if state.has_sprint_autoplay_pending():
		var sk_wk: int = state.week
		var auto_logs: Array[String] = state.apply_skipped_sprint_autoplay()
		var tbl_go := state.consume_team_bonus_log()
		for line in auto_logs:
			session_history.append("[WEEK %d] [АВТОПИЛОТ / неделя без спринта]\n- %s" % [sk_wk, line])
		if tbl_go.strip_edges() != "":
			session_history.append("[WEEK %d] [КОМАНДА]\n- %s" % [sk_wk, tbl_go.replace("\n", " ")])
		_refresh_log_panel_text()
		_set_status_note(
			"Прошла неделя без ручного спринта (отпуск или друг у руля). Смотри лог — затем планируй следующую неделю."
		)
		if state.game_over:
			_show_game_over()
			return

	weekly_tasks = state.get_weekly_tasks()
	selected_tasks.clear()
	event_prepared = false
	planning_locked = false
	current_event.clear()
	card_by_index.clear()
	slot_cards = [null, null, null]
	for s in sprint_slots:
		s.show_empty()
	_render_task_cards()
	_hide_event_until_planning_done()
	_set_status_note("Спринт: перетащи ровно %d карточки в слоты.\nВернуть задачу в бэклог — перетащи карточку обратно в ряд бэклога." % GameState.TASKS_PER_WEEK)
	_update_dashboard()
	waiting_event_choice = false
	choose_a.disabled = true
	choose_b.disabled = true
	next_week_button.disabled = true
	next_week_button.text = "Зафиксировать спринт (3/3)"
	_update_selected_text()
	_maybe_show_stage_intro()

func _render_task_cards() -> void:
	tasks_label.text = "Спринт: бэклог → 3 слота"
	for c in backlog_row.get_children():
		c.queue_free()

	for i in weekly_tasks.size():
		var task: Dictionary = weekly_tasks[i]
		var card := TaskCard.new()
		card.setup(self, state, i, task)
		_style_task_card(card)
		backlog_row.add_child(card)
		card_by_index[i] = card

func _rebuild_selected_tasks_from_slots() -> void:
	selected_tasks.clear()
	for i in 3:
		var c: TaskCard = slot_cards[i]
		if c != null:
			selected_tasks.append(c.task_data)

func _count_slots_filled() -> int:
	var n := 0
	for i in 3:
		if slot_cards[i] != null:
			n += 1
	return n

func _update_selected_text() -> void:
	var n := _count_slots_filled()
	if n == 0:
		status_note = "Слоты пустые. Перетащи %d карточки в слоты спринта." % GameState.TASKS_PER_WEEK
	else:
		var names: Array[String] = []
		for i in 3:
			var c: TaskCard = slot_cards[i]
			if c:
				names.append(str(c.task_data.get("name", "")))
		status_note = "В слотах (%d/3): %s" % [n, ", ".join(names)]

	if n == GameState.TASKS_PER_WEEK:
		status_note += "\nСпринт заполнен. Нажми «Зафиксировать спринт»."
		next_week_button.disabled = false
	else:
		_hide_event_until_planning_done()
		next_week_button.disabled = true

	# Важно: дельты у метрик должны обновляться сразу при drag&drop.
	_update_dashboard()
	var stage: Dictionary = state.get_stage_info()
	status_note += "\nЭтап: %s" % str(stage.get("name", ""))
	status_note += "\nЦель этапа: %s" % str(stage.get("goal", ""))
	status_note += "\nKPI этапа: %s" % str(stage.get("target", ""))
	status_note += "\nПорядок слотов слева→направо задаёт цепочку: предыдущая задача может открыть базу для следующей."
	_refresh_log_panel_text()
	_refresh_task_prereq_visuals()

func _refresh_task_prereq_visuals() -> void:
	_rebuild_selected_tasks_from_slots()
	var names_in_slots: Array[String] = []
	for i in 3:
		var sc: TaskCard = slot_cards[i]
		if sc != null:
			names_in_slots.append(str(sc.task_data.get("name", "")))
	for _k in card_by_index:
		var card: TaskCard = card_by_index[_k]
		var si: int = card.get_slot_index()
		if si < 0:
			card.apply_prereq_visual(false, true)
			continue
		var snap: Dictionary = state.get_snap_after_tasks(selected_tasks, si)
		var met: bool = state.task_prerequisites_met_with(
			card.task_data,
			float(snap["cd"]),
			float(snap["pr"]),
			float(snap["mv"]),
			float(snap["rm"]),
			snap["pp"],
			snap["land"]
		)
		var syn: String = state.synergy_visual_for_sprint_task(str(card.task_data.get("name", "")), names_in_slots)
		card.apply_prereq_visual(not met, false, syn)

func _lock_sprint_drag() -> void:
	for s in sprint_slots:
		s.mouse_filter = Control.MOUSE_FILTER_IGNORE
	for idx in card_by_index.keys():
		var tc: TaskCard = card_by_index[idx]
		tc.mouse_filter = Control.MOUSE_FILTER_IGNORE
	backlog_row.mouse_filter = Control.MOUSE_FILTER_IGNORE


func _unlock_sprint_drag() -> void:
	for s in sprint_slots:
		s.mouse_filter = Control.MOUSE_FILTER_STOP
	for idx in card_by_index.keys():
		var tc: TaskCard = card_by_index[idx]
		tc.mouse_filter = Control.MOUSE_FILTER_STOP
	backlog_row.mouse_filter = Control.MOUSE_FILTER_STOP


func _week_snapshot_state() -> Dictionary:
	return {
		"cash": state.cash,
		"revenue": state.revenue,
		"burn_rate": state.burn_rate,
		"morale": state.morale,
		"tech_debt": state.tech_debt,
		"activation": state.activation,
		"retention": state.retention,
		"reputation": state.reputation,
		"custdev_points": state.custdev_points,
		"problem_clarity": state.problem_clarity,
		"mvp_progress": state.mvp_progress,
		"founder_share_percent": state.founder_share_percent,
		"research_memory": state.research_memory,
		"runway": state.get_runway_months(),
	}


func _ws_money_delta_bbcode(d: float) -> String:
	var n := int(round(d))
	if n > 0:
		return "[color=%s]+%d ₽[/color]" % [POS_COLOR, n]
	if n < 0:
		return "[color=%s]%d ₽[/color]" % [NEG_COLOR, n]
	return "[color=%s]0 ₽[/color]" % NEU_COLOR


func _ws_burn_delta_bbcode(d: float) -> String:
	var n := int(round(d))
	if n > 0:
		return "[color=%s]+%d ₽/мес к burn[/color]" % [NEG_COLOR, n]
	if n < 0:
		return "[color=%s]%d ₽/мес с burn[/color]" % [POS_COLOR, n]
	return "[color=%s]0 ₽/мес[/color]" % NEU_COLOR


func _ws_points_delta_bbcode(d: float) -> String:
	var n := int(round(d))
	if n > 0:
		return "[color=%s]+%d п.[/color]" % [POS_COLOR, n]
	if n < 0:
		return "[color=%s]%d п.[/color]" % [NEG_COLOR, n]
	return "[color=%s]0 п.[/color]" % NEU_COLOR


func _ws_float_delta_bbcode(d: float, unit: String) -> String:
	if absf(d) < 0.04:
		if unit == "":
			return "[color=%s]0[/color]" % NEU_COLOR
		return "[color=%s]0 %s[/color]" % [NEU_COLOR, unit]
	var col := POS_COLOR if d > 0.001 else NEG_COLOR
	if unit == "":
		return "[color=%s]%+.1f[/color]" % [col, d]
	return "[color=%s]%+.1f %s[/color]" % [col, d, unit]


func _week_summary_deltas_bbcode(before: Dictionary, intro: String) -> String:
	var lines: Array[String] = []
	if intro.strip_edges() != "":
		lines.append(intro)
		lines.append("")
	lines.append("[b]Деньги и выживаемость[/b]")
	var c0 := float(before["cash"])
	var c1 := state.cash
	lines.append("  • Кэш  [b]%.0f[/b] → [b]%.0f[/b] ₽  %s" % [c0, c1, _ws_money_delta_bbcode(c1 - c0)])
	var rw0 := float(before["runway"])
	var rw1 := state.get_runway_months()
	lines.append("  • Runway  %.1f → %.1f мес  %s" % [rw0, rw1, _ws_float_delta_bbcode(rw1 - rw0, "мес")])
	var rev0 := float(before["revenue"])
	var rev1 := state.revenue
	lines.append("  • MRR  %.0f → %.0f ₽/мес  %s" % [rev0, rev1, _ws_money_delta_bbcode(rev1 - rev0)])
	var br0 := float(before["burn_rate"])
	var br1 := state.burn_rate
	lines.append("  • Burn  %.0f → %.0f ₽/мес  %s" % [br0, br1, _ws_burn_delta_bbcode(br1 - br0)])
	lines.append("")
	lines.append("[b]Продукт и PMF-база[/b]")
	lines.append(
		"  • Активация / удержание  %.0f / %.0f → %.0f / %.0f  %s  %s"
		% [
			float(before["activation"]),
			float(before["retention"]),
			state.activation,
			state.retention,
			_ws_points_delta_bbcode(state.activation - float(before["activation"])),
			_ws_points_delta_bbcode(state.retention - float(before["retention"])),
		]
	)
	lines.append(
		"  • CD / Problem / MVP  %.0f/%.0f/%.0f → %.0f/%.0f/%.0f  Δ %s · %s · %s"
		% [
			float(before["custdev_points"]),
			float(before["problem_clarity"]),
			float(before["mvp_progress"]),
			state.custdev_points,
			state.problem_clarity,
			state.mvp_progress,
			_ws_points_delta_bbcode(state.custdev_points - float(before["custdev_points"])),
			_ws_points_delta_bbcode(state.problem_clarity - float(before["problem_clarity"])),
			_ws_points_delta_bbcode(state.mvp_progress - float(before["mvp_progress"])),
		]
	)
	var rm0 := float(before["research_memory"])
	var rm1 := state.research_memory
	lines.append("  • Инсайты в памяти  %.1f → %.1f  %s" % [rm0, rm1, _ws_float_delta_bbcode(rm1 - rm0, "")])
	lines.append("")
	lines.append("[b]Команда и риск[/b]")
	lines.append(
		"  • Мораль  %.0f → %.0f  %s"
		% [float(before["morale"]), state.morale, _ws_points_delta_bbcode(state.morale - float(before["morale"]))]
	)
	lines.append(
		"  • Техдолг  %.0f → %.0f  %s"
		% [float(before["tech_debt"]), state.tech_debt, _ws_points_delta_bbcode(state.tech_debt - float(before["tech_debt"]))]
	)
	lines.append(
		"  • Репутация  %.0f → %.0f  %s"
		% [
			float(before["reputation"]),
			state.reputation,
			_ws_points_delta_bbcode(state.reputation - float(before["reputation"])),
		]
	)
	lines.append(
		"  • Доля Мамкина  %.0f%% → %.0f%%  %s"
		% [
			float(before["founder_share_percent"]),
			state.founder_share_percent,
			_ws_points_delta_bbcode(state.founder_share_percent - float(before["founder_share_percent"])),
		]
	)
	return "\n".join(lines)


func _week_summary_progress_bbcode(closed_week: int) -> String:
	var stage: Dictionary = state.get_stage_info()
	var acc := _bb_color_hex(UI_BORDER_ACCENT)
	var lines: Array[String] = []
	lines.append(
		"[font_size=22][b]Неделя %d из %d[/b][/font_size]  [color=%s]%s[/color]"
		% [closed_week, GameState.TOTAL_WEEKS, acc, str(stage.get("name", ""))]
	)
	lines.append("")
	lines.append("[color=%s]%s[/color]" % [NEU_COLOR, str(stage.get("goal", ""))])
	lines.append("[color=%s]KPI: %s[/color]" % [NEU_COLOR, str(stage.get("target", ""))])
	return "\n".join(lines)


func _week_summary_tasks_bbcode(logs: Array[String], team_bonus: String) -> String:
	var lines: Array[String] = []
	lines.append("[b]Спринт — факт[/b]")
	if logs.is_empty():
		lines.append("  • (нет записей)")
	else:
		for l in logs:
			lines.append("  • %s" % str(l).replace("[", "(").replace("]", ")"))
	var tb := team_bonus.strip_edges()
	if tb != "":
		lines.append("")
		lines.append("[b]Команда[/b]")
		for line in tb.split("\n"):
			var t := line.strip_edges()
			if t != "":
				lines.append("  • %s" % t.replace("[", "(").replace("]", ")"))
	return "\n".join(lines)


func _week_summary_hints_bbcode() -> String:
	var bullets: Array[String] = []
	if state.get_runway_months() < 3.0:
		bullets.append(
			"[color=%s]Критично: runway меньше 3 мес — кэш и соотношение MRR/burn в приоритете.[/color]" % NEG_COLOR
		)
	elif state.get_runway_months() < 5.0:
		bullets.append("[color=%s]Внимание: runway меньше 5 мес.[/color]" % _bb_color_hex(UI_TEXT_ACCENT))
	if state.morale < 30.0:
		bullets.append(
			"[color=%s]Низкая мораль — не перегружай техдолгом и «косметическими» победами.[/color]" % _bb_color_hex(UI_TEXT_ACCENT)
		)
	if state.tech_debt > 70.0:
		bullets.append("[color=%s]Высокий техдолг — ретро и упрощение.[/color]" % _bb_color_hex(UI_TEXT_ACCENT))
	if state.launched_to_market and state.retention < 25.0:
		bullets.append(
			"[color=%s]Слабое удержание при выходе в рынок — проверь ценность и онбординг.[/color]" % _bb_color_hex(UI_TEXT_ACCENT)
		)
	if state.founder_share_percent < 55.0 and state.founder_share_percent + 0.001 >= 50.0:
		bullets.append(
			"[color=%s]Доля основателя близка к потере контроля.[/color]" % _bb_color_hex(UI_TEXT_ACCENT)
		)
	var lines: Array[String] = []
	lines.append("[b]На что обратить внимание[/b]")
	if bullets.is_empty():
		lines.append("  [color=%s]Риски в норме — держи баланс роста и runway.[/color]" % POS_COLOR)
	else:
		for b in bullets:
			lines.append("  • " + b)
	return "\n".join(lines)


func _week_summary_set_quiet_event_copy() -> void:
	week_summary_event_section.text = "Пятничный нежданчик"
	week_summary_event_cast.visible = false
	week_summary_event_speaker.visible = false
	week_summary_event_type.text = ""
	week_summary_event_body.text = (
		"[color=%s]На этой неделе без сюрпризов: нежданчик не выпал. Учтены только задачи спринта и обычный такт кассы (MRR − burn, тик инсайтов).[/color]"
		% NEU_COLOR
	)


func _ensure_friday_role_portraits() -> void:
	if _friday_role_portraits_loaded:
		return
	_friday_role_portraits_loaded = true
	var paths: Dictionary = {
		"hr": "res://assets/portrait_hr.png",
		"cfo": "res://assets/portrait_cfo.png",
		"coo": "res://assets/portrait_coo.png",
		"marketer": "res://assets/portrait_cmo.png",
		"cto": "res://assets/portrait_cto.png",
		"friend1": "res://assets/portrait_friend1.png",
		"friend2": "res://assets/portrait_friend2.png",
		"friend3": "res://assets/portrait_friend3.png",
		"startup_fellows_team": "res://assets/startup_fellows_team.png",
		"expert1": "res://assets/expert1.png",
		"expert2": "res://assets/expert2.png",
		"cpo": "res://assets/portrait_cpo.png",
		"investor1": "res://assets/portrait_investor_1.png",
		"investor2": "res://assets/portrait_investor_2.png",
	}
	for k: String in paths.keys():
		var tex: Texture2D = load(paths[k]) as Texture2D
		if tex != null:
			_friday_role_portraits[k] = tex


func _apply_friday_intro_event_art(evt: Dictionary, speaker: Dictionary) -> void:
	friday_intro_art.modulate = Color.WHITE
	var path: String = str(evt.get("illustration", "")).strip_edges()
	if path != "" and path.begins_with("res://") and ResourceLoader.exists(path):
		var t: Texture2D = load(path) as Texture2D
		if t != null:
			friday_intro_art.texture = t
			friday_intro_art.visible = true
			return
	_ensure_friday_role_portraits()
	var pkey: String = str(speaker.get("texture_key", ""))
	if _friday_role_portraits.has(pkey):
		friday_intro_art.texture = _friday_role_portraits[pkey] as Texture2D
		friday_intro_art.visible = true
		return
	friday_intro_art.texture = _resolve_speaker_texture(pkey)
	friday_intro_art.visible = true


func _bind_friday_intro_event_ui(evt: Dictionary) -> void:
	friday_intro_title.text = "Пятничный нежданчик"
	var opts: Array = evt["options"]
	var opt_a: Dictionary = opts[0]
	var opt_b: Dictionary = opts[1]
	var speaker := _event_speaker_profile(evt)
	var a_effects: String = GameState.describe_effects(opt_a["effects"])
	var b_effects: String = GameState.describe_effects(opt_b["effects"])
	var et: String = str(evt.get("type", "Разное"))
	friday_intro_body.bbcode_text = "[b]%s[/b]" % str(evt.get("title", ""))
	friday_intro_speaker.visible = true
	friday_intro_speaker.text = "Говорит: %s · %s" % [str(speaker["name"]), et]
	friday_opt_badge_a.text = "ВАРИАНТ A"
	friday_opt_badge_b.text = "ВАРИАНТ B"
	friday_opt_body_a.bbcode_text = (
		"[b]%s[/b]\n\n[color=%s][i]Эффекты[/i][/color]\n%s"
		% [str(opt_a.get("text", "")), NEU_COLOR, a_effects]
	)
	friday_opt_body_b.bbcode_text = (
		"[b]%s[/b]\n\n[color=%s][i]Эффекты[/i][/color]\n%s"
		% [str(opt_b.get("text", "")), NEU_COLOR, b_effects]
	)
	friday_choose_a.text = "Выбрать вариант A"
	friday_choose_b.text = "Выбрать вариант B"
	friday_choose_a.tooltip_text = a_effects
	friday_choose_b.tooltip_text = b_effects
	_apply_friday_intro_event_art(evt, speaker)


func _week_summary_bind_event_ui(evt: Dictionary) -> void:
	week_summary_event_section.text = "Пятничный нежданчик"
	week_summary_event_cast.visible = true
	week_summary_event_speaker.visible = true
	var opts: Array = evt["options"]
	var opt_a: Dictionary = opts[0]
	var opt_b: Dictionary = opts[1]
	var event_type: String = str(evt.get("type", "Разное"))
	var speaker := _event_speaker_profile(evt)
	week_summary_event_speaker.text = "Говорит: %s" % str(speaker["name"])
	week_summary_event_avatar.texture = _resolve_speaker_texture(str(speaker["texture_key"]))
	week_summary_event_avatar.modulate = speaker["avatar_modulate"] as Color
	week_summary_event_type.text = event_type
	week_summary_event_type.modulate = _color_for_event_type(event_type)
	var a_effects: String = GameState.describe_effects(opt_a["effects"])
	var b_effects: String = GameState.describe_effects(opt_b["effects"])
	week_summary_event_body.text = (
		"[b]%s[/b]\n\n"
		+ "A) %s\n[color=%s]Эффекты:[/color] %s\n\n"
		+ "B) %s\n[color=%s]Эффекты:[/color] %s"
	) % [
		str(evt.get("title", "")),
		str(opt_a.get("text", "")),
		NEU_COLOR,
		a_effects,
		str(opt_b.get("text", "")),
		NEU_COLOR,
		b_effects,
	]
	week_summary_choose_a.text = "Выбрать вариант A"
	week_summary_choose_b.text = "Выбрать вариант B"
	week_summary_choose_a.tooltip_text = a_effects
	week_summary_choose_b.tooltip_text = b_effects


func _week_summary_fill_step2_after_quiet_week() -> void:
	var cw: int = _ws_closed_week
	week_summary_subtitle.text = (
		"Закрыта неделя %d из %d — метрики, задачи и подсказки по итогам спринта." % [cw, GameState.TOTAL_WEEKS]
	)
	week_summary_progress.bbcode_text = _week_summary_progress_bbcode(cw)
	week_summary_tasks.bbcode_text = _week_summary_tasks_bbcode(_ws_task_logs, _ws_team_bonus_log)
	week_summary_deltas.bbcode_text = _week_summary_deltas_bbcode(
		_ws_snap_before,
		"[color=%s][i]Изменения за полную неделю[/i][/color] — задачи спринта и такт кассы; нежданчика на этой неделе не было.\n"
		% NEU_COLOR
	)
	var hints_base: String = _week_summary_hints_bbcode()
	if cw == 1:
		hints_base += (
			"\n\n[color=%s]Со 2-й недели каждую пятницу перед итогами появится выбор по «Пятничному нежданчику».[/color]"
			% NEU_COLOR
		)
	week_summary_hints.bbcode_text = hints_base
	week_summary_col_sep.visible = false
	week_summary_right_column.visible = false


func _show_week_summary_main_step() -> void:
	friday_intro_page.visible = false
	week_summary_main_page.visible = true
	week_summary_continue.visible = true
	week_summary_continue.disabled = false
	week_summary_continue.text = "В следующую неделю"


func _open_week_summary_after_tasks(closed_week: int, has_event: bool) -> void:
	_ws_closed_week = closed_week
	_ws_summary_had_event = has_event
	week_summary_title.text = "Итог недели"
	week_summary_subtitle.text = ""
	friday_intro_page.visible = true
	week_summary_main_page.visible = false
	week_summary_continue.visible = false
	friday_options_row.visible = has_event
	friday_next_button.visible = not has_event
	if has_event:
		_bind_friday_intro_event_ui(current_event)
		friday_choose_a.disabled = false
		friday_choose_b.disabled = false
		_ws_need_event_pick = true
	else:
		friday_intro_title.text = "Пятничный нежданчик"
		friday_intro_body.bbcode_text = _friday_quiet_week_intro_bbcode(closed_week)
		friday_intro_speaker.visible = false
		friday_intro_speaker.text = ""
		friday_intro_art.modulate = Color.WHITE
		_ws_need_event_pick = false
	week_summary_overlay.visible = true
	week_summary_body_scroll.scroll_vertical = 0


func _on_friday_intro_next_pressed() -> void:
	if not week_summary_overlay.visible or _ws_summary_had_event:
		return
	if _ws_need_event_pick:
		return
	_week_summary_fill_step2_after_quiet_week()
	_show_week_summary_main_step()


func _on_week_summary_event_pick(option_idx: int) -> void:
	if not week_summary_overlay.visible or not _ws_need_event_pick:
		return
	var event_log := state.apply_event_option(current_event, option_idx)
	state.finish_week()
	_ws_team_bonus_log = state.consume_team_bonus_log()
	_update_dashboard()
	_append_session_history(option_idx, _ws_reviews, _ws_task_logs, event_log, _ws_team_bonus_log)
	week_summary_subtitle.text = (
		"Закрыта неделя %d из %d — метрики, задачи и подсказки по итогам спринта." % [_ws_closed_week, GameState.TOTAL_WEEKS]
	)
	week_summary_progress.bbcode_text = _week_summary_progress_bbcode(_ws_closed_week)
	week_summary_tasks.bbcode_text = _week_summary_tasks_bbcode(_ws_task_logs, _ws_team_bonus_log)
	week_summary_deltas.bbcode_text = _week_summary_deltas_bbcode(
		_ws_snap_before,
		"[color=%s][i]Полный итог недели[/i][/color] — задачи, нежданчик и такт кассы.\n" % NEU_COLOR
	)
	var opts: Array = current_event.get("options", [])
	var opt_text := ""
	if option_idx >= 0 and option_idx < opts.size():
		opt_text = str((opts[option_idx] as Dictionary).get("text", ""))
	var hints_tail: String = "\n\n[color=%s][b]Твой выбор в нежданчике:[/b] %s[/color]" % [POS_COLOR, opt_text]
	week_summary_hints.bbcode_text = _week_summary_hints_bbcode() + hints_tail
	_ws_need_event_pick = false
	waiting_event_choice = false
	week_summary_choose_a.disabled = true
	week_summary_choose_b.disabled = true
	week_summary_event_buttons.visible = false
	friday_choose_a.disabled = true
	friday_choose_b.disabled = true
	friday_options_row.visible = false
	week_summary_col_sep.visible = false
	week_summary_right_column.visible = false
	_show_week_summary_main_step()
	week_summary_body_scroll.scroll_vertical = 0
	_set_status_note("%s\n\nНеделя закрыта. Нажми «В следующую неделю»." % event_log)


func _on_week_summary_continue_pressed() -> void:
	if not week_summary_overlay.visible or _ws_need_event_pick:
		return
	week_summary_overlay.visible = false
	_stage_presentation_lock_key = ""
	_unlock_sprint_drag()
	prepare_week()


func _style_friday_nezhdan_option_columns() -> void:
	var a_bg: Color = UI_SLOT_EMPTY.darkened(0.02)
	var a_br: Color = UI_BORDER_ACCENT
	var b_bg: Color = UI_PANEL_EVENT.lightened(0.03)
	var b_br: Color = UI_TEXT_ACCENT
	friday_opt_panel_a.add_theme_stylebox_override(
		"panel", _pixel_panel(_glass(a_bg, 0.88), _glass(a_br, 0.52), 14)
	)
	friday_opt_panel_b.add_theme_stylebox_override(
		"panel", _pixel_panel(_glass(b_bg, 0.88), _glass(b_br, 0.5), 14)
	)
	friday_opt_badge_a.add_theme_color_override("font_color", a_br.lightened(0.12))
	friday_opt_badge_b.add_theme_color_override("font_color", b_br.lightened(0.08))
	friday_opt_badge_a.add_theme_font_size_override("font_size", 18)
	friday_opt_badge_b.add_theme_font_size_override("font_size", 18)
	_style_button_outline(
		friday_choose_a, UI_BTN_PRIMARY_BG.darkened(0.03), UI_BTN_PRIMARY_BG_H, _glass(a_br, 0.65)
	)
	_style_button_outline(
		friday_choose_b, UI_BTN_CTA_BG.darkened(0.02), UI_BTN_CTA_BG_H, _glass(b_br, 0.62)
	)


func _style_week_summary_overlay() -> void:
	week_summary_overlay.mouse_filter = Control.MOUSE_FILTER_STOP
	week_summary_dialog.add_theme_stylebox_override(
		"panel",
		_rounded_glass_panel(UI_PANEL_CENTER.lightened(0.04), UI_BORDER_ACCENT.darkened(0.2), 16, 20, 0.56, 0.62)
	)
	week_summary_title.add_theme_color_override("font_color", UI_TEXT_ACCENT)
	week_summary_subtitle.add_theme_color_override("font_color", UI_TEXT_MUTED)
	friday_intro_title.add_theme_color_override("font_color", UI_TEXT_ACCENT)
	week_summary_left_heading.add_theme_color_override("font_color", UI_TEXT_ACCENT)
	for rtl in [
		week_summary_progress,
		week_summary_tasks,
		week_summary_deltas,
		week_summary_hints,
		week_summary_event_body,
		friday_intro_body,
		friday_opt_body_a,
		friday_opt_body_b,
	]:
		(rtl as RichTextLabel).add_theme_color_override("default_color", UI_TEXT)
	friday_intro_speaker.add_theme_color_override("font_color", UI_TEXT_MUTED)
	week_summary_event_section.add_theme_color_override("font_color", UI_TEXT_ACCENT)
	week_summary_event_speaker.add_theme_color_override("font_color", UI_TEXT_MUTED)
	week_summary_event_type.add_theme_color_override("font_color", UI_TEXT)
	_style_button_outline(week_summary_choose_a, UI_BTN_PRIMARY_BG, UI_BTN_PRIMARY_BG_H, UI_BORDER)
	_style_button_outline(week_summary_choose_b, UI_BTN_PRIMARY_BG, UI_BTN_PRIMARY_BG_H, UI_BORDER)
	_style_friday_nezhdan_option_columns()
	_style_button_cta(week_summary_continue)
	_style_button_cta(friday_next_button)


func _on_next_week_pressed() -> void:
	if week_summary_overlay.visible:
		return
	if not planning_locked:
		if _count_slots_filled() != GameState.TASKS_PER_WEEK:
			_set_status_note("Нужно заполнить все 3 слота карточками.")
			return
		_rebuild_selected_tasks_from_slots()
		if not state.sprint_has_required_grant_report(selected_tasks):
			_set_status_note("Для текущей недели обязательна задача «Сдать отчетность по гранту». Пока она не выбрана, спринт не фиксируется.")
			return
		_stage_presentation_lock_key = _normalized_stage_key(str(state.get_stage_info().get("key", "build")))
		planning_locked = true
		_rebuild_selected_tasks_from_slots()
		_lock_sprint_drag()
		_prepare_event_if_needed()
		_ws_snap_before = _week_snapshot_state()
		var closing_week: int = state.week
		_ws_reviews = _build_task_reviews_for_selection()
		_ws_task_logs = state.apply_tasks(selected_tasks)
		_update_dashboard()
		if state.game_over:
			var qlog := "Нежданчик не сыгран: партия завершена после задач."
			state.finish_week()
			var tbl_go := state.consume_team_bonus_log()
			_append_session_history(-1, _ws_reviews, _ws_task_logs, qlog, tbl_go)
			_stage_presentation_lock_key = ""
			_unlock_sprint_drag()
			_set_status_note("\n".join(_ws_task_logs))
			prepare_week()
			return
		var has_event: bool = not current_event.is_empty()
		next_week_button.disabled = true
		next_week_button.text = "Спринт зафиксирован"
		if not has_event:
			state.finish_week()
			_ws_team_bonus_log = state.consume_team_bonus_log()
			var quiet_log := "Нежданчик: этой недели без сюрприза."
			_append_session_history(-1, _ws_reviews, _ws_task_logs, quiet_log, _ws_team_bonus_log)
			waiting_event_choice = false
			_set_status_note("Неделя %d закрыта — итоги во всплывающем окне." % closing_week)
			_open_week_summary_after_tasks(closing_week, false)
			return
		waiting_event_choice = true
		_ws_team_bonus_log = ""
		_set_status_note("Неделя %d: итоги и нежданчик — во всплывающем окне." % closing_week)
		_open_week_summary_after_tasks(closing_week, true)
	else:
		_set_status_note("Спринт уже зафиксирован. Заверши окно итогов недели.")

func _game_over_texture_for_key(k: String) -> Texture2D:
	var path: String = str(GAME_OVER_ART.get(k, GAME_OVER_ART["burnout"]))
	if ResourceLoader.exists(path):
		return load(path) as Texture2D
	return game_over_art.texture


func _game_over_advice_bbcode(key: String) -> String:
	match key:
		"bankrupt":
			return "Держи запас денег и runway: при [b]cash ≤ 0[/b] в спринте нужна задача «Срочно: бридж-раунд» или срочное сокращение burn. Иначе неделя не закрывается."
		"scold":
			return "На финиш важно уйти с положительной кассой: урежь burn, подними MRR или заключи сделку до исчерпания недель."
		"dilution":
			return "Следи за долей основателя: слишком много инвестиций и найма за долю приводят к потере контроля (менее 50% акций). Балансируй деньги и equity."
		"burnout":
			return "Мораль падает от перегруза и плохих решений. Давай команде передышку, не гонись за задачами «любой ценой», чинь техдолг и отношения."
		"triumph":
			return "Чтобы повторить успех: последовательно качай [b]активацию и удержание[/b], следи за кассой и не распускай burn на этапе роста."
		"struggle":
			return "Дожить до дедлайна мало — наращивай [b]PMF[/b]: интервью, метрики продукта, фокус на одном сегменте и проверка гипотез каждую неделю."
		_:
			return "Попробуй другую комбинацию задач и ответов в нежданчике — причина поражения уже в тексте выше."


func _populate_game_over_overlay() -> void:
	var key: String = str(state.game_over_art_key)
	game_over_art.texture = _game_over_texture_for_key(key)
	game_over_title.text = "Поздравляем!" if key == "triumph" else "Партия завершена"
	game_over_name.text = "Мамкин"
	game_over_reason.bbcode_text = "[b]Что произошло[/b]\n%s" % str(state.game_over_reason)
	game_over_advice.bbcode_text = _game_over_advice_bbcode(key)
	game_over_advice_heading.visible = key != "triumph"
	game_over_advice.visible = key != "triumph"
	if key == "triumph":
		game_over_hint.text = "Победная концовка по метрикам. Сохрани лог или начни партию заново — кнопки ниже."
	else:
		game_over_hint.text = "Сохрани лог партии или начни заново — кнопки ниже. Подробности — «📘 Лог» на верхней панели."


func _on_game_over_play_again_pressed() -> void:
	get_tree().reload_current_scene()


func _style_game_over_overlay() -> void:
	game_over_overlay.mouse_filter = Control.MOUSE_FILTER_STOP
	game_over_dialog.add_theme_stylebox_override(
		"panel",
		_rounded_glass_panel(UI_PANEL_CENTER.lightened(0.04), UI_BORDER_ACCENT.darkened(0.2), 16, 20, 0.56, 0.62)
	)
	game_over_title.add_theme_color_override("font_color", UI_TEXT_ACCENT)
	game_over_name.add_theme_color_override("font_color", UI_TEXT_MUTED)
	game_over_reason.add_theme_color_override("default_color", UI_TEXT)
	game_over_advice_heading.add_theme_color_override("font_color", UI_TEXT_ACCENT)
	game_over_advice.add_theme_color_override("default_color", UI_TEXT)
	game_over_hint.add_theme_color_override("font_color", UI_TEXT_MUTED)
	_style_button_outline(game_over_save_log, UI_BTN_PRIMARY_BG, UI_BTN_PRIMARY_BG_H, UI_BORDER)
	_style_button_cta(game_over_play_again)


func _show_game_over() -> void:
	week_summary_overlay.visible = false
	_populate_game_over_overlay()
	_play_game_over_music_for_art_key(str(state.game_over_art_key))
	game_over_overlay.visible = true
	tasks_label.text = "Партия завершена."
	_set_status_note("Конец партии — окно с итогом на экране.")
	week_summary_choose_a.disabled = true
	week_summary_choose_b.disabled = true
	friday_choose_a.disabled = true
	friday_choose_b.disabled = true
	friday_next_button.disabled = true
	week_summary_continue.disabled = true
	next_week_button.disabled = true
	launch_now_button.visible = false
	launch_now_button.disabled = true
	_update_dashboard()

func _hide_event_until_planning_done() -> void:
	pass

func _prepare_event_if_needed() -> void:
	if event_prepared:
		return
	current_event = state.pick_event()
	event_prepared = true

func _role_display_name(role_key: String) -> String:
	match role_key:
		"CMO":
			return "CMO"
		"CTO":
			return "CTO"
		"CFO":
			return "CFO"
		"HR":
			return "HR"
		"COO":
			return "COO"
		"CCO":
			return "CCO"
		"CPO":
			return "CPO"
		"FRIEND":
			return "Друг с долей"
		_:
			return role_key

func _event_speaker_profile(event_data: Dictionary) -> Dictionary:
	var speaker_tex_override: String = str(event_data.get("speaker_texture", "")).strip_edges()
	if speaker_tex_override != "":
		var nm: String = str(event_data.get("speaker_name", "Эксперт"))
		return {"name": nm, "texture_key": speaker_tex_override, "avatar_modulate": Color(0.9, 0.92, 0.96, 1.0)}
	var scr: String = str(event_data.get("script", ""))
	if scr == "grant_verdict":
		return {"name": "Грант (без доли)", "texture_key": "cfo", "avatar_modulate": Color(1.0, 0.86, 0.56, 1.0)}
	if scr == "accelerator_verdict":
		var ek: String = "expert2" if (state.week & 1) == 1 else "expert1"
		return {"name": "Акселератор / менторы", "texture_key": ek, "avatar_modulate": Color(0.88, 0.9, 0.96, 1.0)}
	var req_roles: Array = event_data.get("requires_roles", [])
	if not req_roles.is_empty():
		var role: String = str(req_roles[0])
		var tone := Color(0.78, 0.88, 1.0, 1.0)
		var tex_key := "mamkin"
		if role == "CFO":
			tone = Color(1.0, 0.86, 0.56, 1.0)
			tex_key = "cfo"
		elif role == "CTO":
			tone = Color(0.76, 1.0, 0.86, 1.0)
			tex_key = "cto"
		elif role == "CMO":
			tone = Color(1.0, 0.72, 0.78, 1.0)
			tex_key = "marketer"
		elif role == "CCO":
			tone = Color(1.0, 0.78, 0.62, 1.0)
			tex_key = "cco"
		elif role == "CPO":
			tone = Color(0.82, 0.92, 1.0, 1.0)
			tex_key = "cpo"
		elif role == "HR":
			tex_key = "hr"
		elif role == "COO":
			tex_key = "coo"
		elif role == "FRIEND":
			var fvn: int = clampi(int(round(float(event_data.get("friend_portrait", 1)))), 1, 3)
			tex_key = "friend%d" % fvn
		return {
			"name": _role_display_name(role),
			"texture_key": tex_key,
			"avatar_modulate": tone
		}
	if bool(event_data.get("requires_friend", false)):
		var fv2: int = clampi(int(round(float(event_data.get("friend_portrait", 1)))), 1, 3)
		var fk: String = "friend%d" % fv2
		return {"name": "Друг с долей", "texture_key": fk, "avatar_modulate": Color(0.95, 0.95, 1.0, 1.0)}
	var event_type: String = str(event_data.get("type", "Разное"))
	if event_type == "Адвайзер" or event_type == "Эксперт":
		var ek: String = "expert2" if (state.week & 1) == 1 else "expert1"
		return {"name": "Эксперт", "texture_key": ek, "avatar_modulate": Color(0.88, 0.9, 0.96, 1.0)}
	if event_type == "Инвестор":
		var ip: int = int(round(float(event_data.get("investor_portrait", 1))))
		if ip < 1 or ip > 2:
			ip = 1
		var ik: String = "investor%d" % ip
		return {"name": "Инвестор", "texture_key": ik, "avatar_modulate": Color(0.92, 0.88, 1.0, 1.0)}
	if event_type == "Команда":
		return {"name": "Команда", "texture_key": "startup_fellows_team", "avatar_modulate": Color(0.95, 0.95, 1.0, 1.0)}
	if event_type == "Продукт":
		return {"name": "Продукт", "texture_key": "mamkin", "avatar_modulate": Color(0.88, 0.94, 1.0, 1.0)}
	if event_type == "Техника":
		return {"name": "Техлид", "texture_key": "cto", "avatar_modulate": Color(0.76, 0.94, 1.0, 1.0)}
	if event_type == "Рынок":
		return {"name": "Go-to-Market", "texture_key": "marketer", "avatar_modulate": Color(1.0, 0.86, 0.72, 1.0)}
	if event_type == "PR/Репутация":
		return {"name": "PR/Коммуникации", "texture_key": "marketer", "avatar_modulate": Color(1.0, 0.78, 0.84, 1.0)}
	return {"name": "Команда", "texture_key": "startup_fellows_team", "avatar_modulate": Color(0.92, 0.92, 0.92, 1.0)}

func _resolve_speaker_texture(texture_key: String) -> Texture2D:
	_ensure_friday_role_portraits()
	if _friday_role_portraits.has(texture_key):
		return _friday_role_portraits[texture_key] as Texture2D
	match texture_key:
		"investor":
			return event_avatar_investor_tex
		"cto":
			return event_avatar_cto_tex
		"marketer":
			return event_avatar_marketer_tex
		"cfo":
			return event_avatar_cfo_tex
		"hr":
			return event_avatar_hr_tex
		"coo":
			return event_avatar_coo_tex
		"cco":
			return event_avatar_cco_tex
		"friend1", "friend2", "friend3":
			return event_avatar_mamkin_tex
		"startup_fellows_team":
			return event_avatar_mamkin_tex
		_:
			return event_avatar_mamkin_tex

func _selected_task_names() -> Array[String]:
	var names: Array[String] = []
	for t in selected_tasks:
		names.append(str(t.get("name", "")))
	return names

func _task_review_line(task: Dictionary, applied: bool, p_cd: float, p_pr: float, p_mv: float, p_rm: float, p_pp: Variant = null, p_pl: Variant = null) -> String:
	var tname: String = str(task.get("name", ""))
	if not applied:
		var why: String = state.describe_prereq_gap(task, p_cd, p_pr, p_mv, p_rm, p_pp, p_pl)
		return "- ⚠ Преждевременно: %s — %s." % [tname, why]
	var stage_key: String = str(state.get_stage_info().get("key", "build"))
	var effects: Dictionary = task.get("effects", {})
	if stage_key == "build":
		if effects.has("hire_cco"):
			return "- ℹ Ранний найм: %s — CCO логичнее на Fit, но на Build можно заранее собрать продажи до релиза." % tname
		if effects.has("hire_cpo"):
			return "- ℹ Ранний найм: %s — CPO особенно полезен в рынке; на Build всё равно ускоряет ясность продукта и MVP." % tname
		if effects.has("launch_market"):
			if state.launch_prerequisites_met_recommended():
				return "- ✅ Своевременно: %s — уверенный выход из Build в рынок (сильный старт на Fit)." % tname
			return "- ⚠ Ранний выход: %s — Fit открыт, но старт слабее; на этапе придётся догонять продукт и команду." % tname
		if effects.has("custdev_points") or effects.has("problem_clarity") or effects.has("mvp_progress") or effects.has("hire_hr") or effects.has("hire_marketer") or effects.has("hire_cto") or effects.has("hire_cfo") or effects.has("hire_coo") or effects.has("hire_cpo"):
			return "- ✅ Своевременно: %s — усиливает базу Build (команда/проблема/MVP)." % tname
		if effects.has("rent_office_a") and state.problem_clarity < 6.0:
			return "- ❌ Напрасно: %s — дорогой офис до ясной проблемы обычно ускоряет burn, а не PMF." % tname
		if effects.has("hire_friend") and state.problem_clarity < 6.0:
			return "- ❌ Напрасно: %s — дорогой найм с долей до подтверждения проблемы." % tname
		return "- ℹ Нейтрально: %s — допустимо, но влияние на главные KPI Build умеренное." % tname
	if stage_key == "fit":
		if effects.has("hire_cco"):
			return "- ✅ Своевременно: %s — руководитель продаж уместен на этапе Fit." % tname
		if effects.has("hire_cpo"):
			return "- ✅ Своевременно: %s — продуктолог помогает удержанию и активации на Fit." % tname
		if effects.has("activation") or effects.has("retention"):
			return "- ✅ Своевременно: %s — работает на PMF-метрики Fit." % tname
		if effects.has("revenue") and (effects.has("retention") or effects.has("activation")):
			return "- ✅ Своевременно: %s — рост с поддержкой продуктовых метрик." % tname
		return "- ℹ Нейтрально: %s — задача не вредит, но слабо двигает PMF-переход." % tname
	if effects.has("revenue") or effects.has("reputation"):
		return "- ✅ Своевременно: %s — релевантно целям Scale." % tname
	return "- ℹ Нейтрально: %s — влияние на цели Scale ограничено." % tname

func _build_task_reviews_for_selection() -> Array[String]:
	var out: Array[String] = []
	_rebuild_selected_tasks_from_slots()
	for i in selected_tasks.size():
		var before: Dictionary = state.get_snap_after_tasks(selected_tasks, i)
		var task: Dictionary = selected_tasks[i]
		var applied: bool = state.task_prerequisites_met_with(
			task,
			float(before["cd"]),
			float(before["pr"]),
			float(before["mv"]),
			float(before["rm"]),
			before["pp"],
			before["land"]
		)
		out.append(_task_review_line(task, applied, float(before["cd"]), float(before["pr"]), float(before["mv"]), float(before["rm"]), before["pp"], before["land"]))
	return out

func _append_session_history(option_idx: int, reviews: Array[String], task_logs: Array[String], event_log: String, team_bonus_log: String) -> void:
	var opts: Array = current_event.get("options", [])
	var opt_text: String = ""
	if option_idx >= 0 and option_idx < opts.size():
		opt_text = str((opts[option_idx] as Dictionary).get("text", ""))
	var entry: Array[String] = []
	entry.append("[WEEK %d] Этап: %s" % [state.week - 1, str(state.get_stage_info().get("name", ""))])
	entry.append("Задачи: %s" % ", ".join(_selected_task_names()))
	entry.append("Нежданчик: %s" % event_log)
	entry.append("Выбор: %s" % opt_text)
	entry.append("Оценка решений:")
	for r in reviews:
		entry.append(r)
	entry.append("Фактический результат недели:")
	for l in task_logs:
		entry.append("- %s" % l)
	if team_bonus_log != "":
		entry.append("- %s" % team_bonus_log.replace("\n", " "))
	session_history.append("\n".join(entry))
	if session_history.size() > 40:
		session_history = session_history.slice(session_history.size() - 40, session_history.size())
	_refresh_log_panel_text()

func _update_dashboard() -> void:
	_apply_stage_background()
	_apply_stage_ui_theme()
	_apply_stage_background_music()
	var pulse := _pulse_metrics_compact_bbcode() if pulse_compact_mode else _pulse_metrics_expanded_bbcode()
	metrics_left.bbcode_text = pulse[0]
	metrics_right.bbcode_text = pulse[1]
	_refresh_achievement_icons_ui()
	risk_badge_label.text = _build_risk_badges()
	_update_risk_visuals()
	_update_launch_button()


func _achievement_group_shell_style() -> StyleBoxFlat:
	var sb := StyleBoxFlat.new()
	sb.bg_color = Color(0, 0, 0, 0)
	sb.border_width_left = 0
	sb.border_width_top = 0
	sb.border_width_right = 0
	sb.border_width_bottom = 0
	sb.content_margin_left = 0
	sb.content_margin_top = 0
	sb.content_margin_right = 0
	sb.content_margin_bottom = 0
	return sb


func _refresh_achievement_icons_ui() -> void:
	if achievement_groups_root == null:
		return
	while achievement_groups_root.get_child_count() > 0:
		achievement_groups_root.get_child(0).free()
	achievements_scroll.horizontal_scroll_mode = ScrollContainer.SCROLL_MODE_AUTO
	achievements_scroll.vertical_scroll_mode = ScrollContainer.SCROLL_MODE_DISABLED

	var grouped: Array = _collect_active_achievement_groups()
	if grouped.is_empty():
		var empty_note := Label.new()
		empty_note.text = "Пока нет активных ачивок."
		empty_note.add_theme_color_override("font_color", UI_TEXT_MUTED)
		empty_note.autowrap_mode = TextServer.AUTOWRAP_WORD_SMART
		achievement_groups_root.add_child(empty_note)
		return

	var groups_row := HBoxContainer.new()
	groups_row.size_flags_horizontal = Control.SIZE_SHRINK_BEGIN
	groups_row.add_theme_constant_override("separation", 8)

	for block in grouped:
		var shell := PanelContainer.new()
		shell.size_flags_horizontal = Control.SIZE_SHRINK_BEGIN
		shell.add_theme_stylebox_override("panel", _achievement_group_shell_style())

		var inner := MarginContainer.new()
		inner.add_theme_constant_override("margin_left", 2)
		inner.add_theme_constant_override("margin_top", 2)
		inner.add_theme_constant_override("margin_right", 2)
		inner.add_theme_constant_override("margin_bottom", 2)

		var flow := HBoxContainer.new()
		flow.add_theme_constant_override("separation", 4)

		for raw_entry in block["entries"]:
			var entry: Dictionary = raw_entry as Dictionary
			var tex_path: String = str(entry.get("texture_path", ""))
			var tip: String = str(entry.get("tooltip", ""))
			flow.add_child(_make_achievement_icon_tile(tex_path, tip))

		inner.add_child(flow)
		shell.add_child(inner)
		groups_row.add_child(shell)

	achievement_groups_root.add_child(groups_row)


func _make_achievement_icon_tile(texture_path: String, tooltip_txt: String) -> Control:
	var holder := PanelContainer.new()
	holder.mouse_filter = Control.MOUSE_FILTER_STOP
	holder.tooltip_text = tooltip_txt.strip_edges()
	holder.custom_minimum_size = ACHIEVEMENT_ICON_DISPLAY_SIZE

	var inset := StyleBoxFlat.new()
	inset.bg_color = Color(0.06, 0.08, 0.11, 0.55)
	inset.border_width_left = 1
	inset.border_width_top = 1
	inset.border_width_right = 1
	inset.border_width_bottom = 1
	inset.border_color = Color(1, 1, 1, 0.12)
	inset.corner_radius_top_left = 8
	inset.corner_radius_top_right = 8
	inset.corner_radius_bottom_right = 8
	inset.corner_radius_bottom_left = 8
	inset.content_margin_left = 4
	inset.content_margin_top = 4
	inset.content_margin_right = 4
	inset.content_margin_bottom = 4
	holder.add_theme_stylebox_override("panel", inset)

	var cc := CenterContainer.new()
	cc.set_anchors_preset(Control.PRESET_FULL_RECT)
	cc.mouse_filter = Control.MOUSE_FILTER_IGNORE

	var ok_path: bool = texture_path != "" and ResourceLoader.exists(texture_path)
	if ok_path:
		var tr := TextureRect.new()
		tr.texture = load(texture_path)
		tr.expand_mode = TextureRect.EXPAND_IGNORE_SIZE
		tr.stretch_mode = TextureRect.STRETCH_KEEP_ASPECT_CENTERED
		tr.custom_minimum_size = ACHIEVEMENT_ICON_DISPLAY_SIZE - Vector2(10, 10)
		cc.add_child(tr)
	else:
		var ph := Label.new()
		ph.text = "?"
		ph.add_theme_font_size_override("font_size", 22)
		ph.add_theme_color_override("font_color", UI_TEXT_MUTED)
		cc.add_child(ph)

	holder.add_child(cc)
	return holder


func _resolve_achievement_entry(display_item: String) -> Dictionary:
	var item: String = display_item.strip_edges()
	var tex: String = ""
	var tip: String = item

	match item:
		"Startup Fellows":
			tex = "res://assets/icons/icon_team_startup_fellows_64.png"
			tip = "Подключена программа Startup Fellows."
		"HR":
			tex = "res://assets/icons/icon_team_hr_64.png"
			tip = "В команде есть HR."
		"CMO":
			tex = "res://assets/icons/icon_team_cmo_64.png"
			tip = "В команде есть CMO (маркетинг)."
		"CTO":
			tex = "res://assets/icons/icon_team_cto_64.png"
			tip = "В команде есть CTO."
		"CFO":
			tex = "res://assets/icons/icon_team_cfo_64.png"
			tip = "В команде есть CFO."
		"COO":
			tex = "res://assets/icons/icon_team_coo_64.png"
			tip = "В команде есть COO."
		"CCO":
			tex = "res://assets/icons/icon_team_cco_64.png"
			tip = "В команде есть CCO (выручка)."
		"CPO":
			tex = "res://assets/icons/icon_team_cpo_64.png"
			tip = "В команде есть CPO."
		"Home Office":
			tex = "res://assets/icons/icon_infra_home_office_64.png"
			tip = "Офис: работа из дома (нет арендуемого офиса)."
		"Офис B":
			tex = "res://assets/icons/icon_infra_office_b_64.png"
			tip = "Офис класса B."
		"Офис A":
			tex = "res://assets/icons/icon_infra_office_a_64.png"
			tip = "Офис класса A."
		"Смузи-бар":
			tex = "res://assets/icons/icon_infra_smoothie_bar_64.png"
			tip = "В офисе есть смузи-бар."
		"Кофе-леди":
			tex = "res://assets/icons/icon_infra_coffee_lady_64.png"
			tip = "В офисе есть кофе-сервис (кофе-леди)."
		"Массажист":
			tex = "res://assets/icons/icon_infra_masseur_64.png"
			tip = "В офисе есть массажист."
		"Экспертный контур":
			tex = ""
			tip = "Построена линия работы с экспертами и адвайзерами."
		"Заявка на грант":
			tex = ""
			tip = "Подана заявка на грант; ждём вердикт в нежданчике."
		"Отчётность обязательна":
			tex = ""
			tip = "Требуется отчётность по гранту."
		"Отчётность на проверке":
			tex = ""
			tip = "Отчётность по гранту отправлена на проверку."
		"Заявка в акселератор":
			tex = ""
			tip = "Подана заявка в акселератор."
		"Growth-агентство":
			tex = ""
			tip = "Подключено growth-агентство."
		"Инфлюенсер-канал":
			tex = ""
			tip = "Есть канал через инфлюенсеров."
		"Telegram-канал":
			tex = ""
			tip = "Есть telegram-канал про стартап."
		"Инвестконтакт":
			tex = ""
			tip = "Есть контакт с инвесторами."
		"Посадочная и оферта":
			tip = "Есть посадочная и оферта (правовая база лендинга)."
			tex = ""
		"В рынке":
			tip = "Продукт выведен на рынок."
			tex = ""
		"Комплаенс-база":
			tip = "Сформирована база комплаенса (репутация и удержание)."
			tex = ""
		"Прототип":
			tip = "Есть играбельный прототип продукта."
			tex = ""
		"Тех. MVP":
			tip = "Есть технический MVP (backend/данные)."
			tex = ""
		"Запуск в рынок":
			tip = "Запуск в рынок уже произошёл."
			tex = ""
		_:
			pass

	if item.begins_with("Друг x"):
		tex = "res://assets/icons/icon_team_friend_64.png"
		tip = "В команде есть друг: %s." % item

	if item.begins_with("Грант x"):
		tip = "Выигран грант (учёт): %s." % item

	if item.begins_with("Акселератор"):
		tip = "Акселератор активен: %s." % item

	if item == "Выручка >= 1k":
		tip = "MRR достиг порога 1 000 ₽/мес."
	if item == "Выручка >= 5k":
		tip = "MRR достиг порога 5 000 ₽/мес."
	if item == "Кэш >= 100k (вернули)":
		tip = "После провала ниже порога удалось снова набрать ≥ 100 000 ₽ на счёте."
	if item == "Кэш >= 250k":
		tip = "На счёте не менее 250 000 ₽."

	if item.begins_with("CustDev "):
		tip = "CustDev: проведённые интервью (%s)." % item
	if item.begins_with("Пилоты "):
		tip = "Шагов пилота выполнено (%s)." % item
	if item.begins_with("Тесты прототипа "):
		tip = "Пользователей в прототипных тестах (%s)." % item
	if item.begins_with("Холодные касания "):
		tip = "Холодные касания выполнены (%s)." % item
	if item.begins_with("Экспертные интервью "):
		tip = "Интервью с экспертами проведены (%s)." % item

	return {"texture_path": tex, "tooltip": tip}


func _collect_active_achievement_groups() -> Array:
	var groups_in: Array[Dictionary] = []
	groups_in.append({
		"name": "Команда",
		"items": [
			"Startup Fellows" if state.startup_fellows_enabled else "",
			"HR" if state.has_hr else "",
			"CMO" if state.has_marketer else "",
			"CTO" if state.has_cto else "",
			"CFO" if state.has_cfo else "",
			"COO" if state.has_coo else "",
			"CCO" if state.has_cco else "",
			"CPO" if state.has_cpo else "",
			("Друг x%d" % state.friend_count) if state.friend_count > 0 else "",
		]
	})
	groups_in.append({
		"name": "Инфраструктура",
		"items": [
			"Home Office" if state.office_tier == 0 else "",
			"Офис B" if state.office_tier == 1 else "",
			"Офис A" if state.office_tier >= 2 else "",
			"Смузи-бар" if state.has_smoothie_bar else "",
			"Кофе-леди" if state.has_coffee_lady else "",
			"Массажист" if state.has_office_masseur else "",
		]
	})
	groups_in.append({
		"name": "Эксперты",
		"items": ["Экспертный контур" if state.has_advisor_pipeline else ""]
	})
	groups_in.append({
		"name": "Гранты и акселератор",
		"items": [
			"Заявка на грант" if state.pending_grant_review else "",
			("Грант x%d" % state.grant_wins_count) if state.grant_wins_count > 0 else "",
			"Отчётность обязательна" if state.grant_report_required else "",
			"Отчётность на проверке" if state.pending_grant_report_review else "",
			"Заявка в акселератор" if state.pending_accelerator_review else "",
			("Акселератор %d нед." % state.accelerator_program_weeks_left) if state.accelerator_program_weeks_left > 0 else "",
		]
	})
	groups_in.append({
		"name": "Партнёрки и каналы",
		"items": [
			"Growth-агентство" if state.has_growth_agency_contract else "",
			"Инфлюенсер-канал" if state.has_influencer_channel else "",
			"Telegram-канал" if state.has_startup_telegram_channel else "",
			"Инвестконтакт" if state.has_investor_contact else "",
		]
	})
	groups_in.append({
		"name": "Юридический блок",
		"items": [
			"Посадочная и оферта" if state.has_product_landing else "",
			"В рынке" if state.launched_to_market else "",
			"Комплаенс-база" if state.reputation >= 35.0 and state.retention >= 20.0 else "",
		]
	})
	groups_in.append({
		"name": "Финансы",
		"items": [
			"Выручка >= 1k" if state.revenue >= 1000.0 else "",
			"Выручка >= 5k" if state.revenue >= 5000.0 else "",
			"Кэш >= 100k (вернули)" if state.week > 1 and state.has_seen_cash_below_100k and state.cash >= 100000.0 else "",
			"Кэш >= 250k" if state.cash >= 250000.0 else "",
		]
	})
	groups_in.append({
		"name": "Продукт",
		"items": [
			"Прототип" if state.has_playable_prototype else "",
			"Тех. MVP" if state.has_technical_mvp else "",
			"Запуск в рынок" if state.launched_to_market else "",
			("CustDev %d" % state.stat_custdev_interviews_done) if state.stat_custdev_interviews_done > 0 else "",
			("Пилоты %d" % state.stat_pilot_steps_done) if state.stat_pilot_steps_done > 0 else "",
			("Тесты прототипа %d" % state.stat_prototype_users_tested) if state.stat_prototype_users_tested > 0 else "",
			("Холодные касания %d" % state.stat_cold_outreach_count) if state.stat_cold_outreach_count > 0 else "",
			("Экспертные интервью %d" % state.stat_expert_interviews_count) if state.stat_expert_interviews_count > 0 else "",
		]
	})

	var out: Array = []
	for g in groups_in:
		var entries: Array = []
		for raw_item in g["items"]:
			var item: String = str(raw_item).strip_edges()
			if item == "":
				continue
			entries.append(_resolve_achievement_entry(item))
		if entries.is_empty():
			continue
		out.append({"name": str(g["name"]), "entries": entries})
	return out

func _update_launch_button() -> void:
	if launch_now_button == null:
		return
	var can: bool = state.can_launch_now() and not state.launched_to_market and not state.game_over
	var busy: bool = planning_locked or waiting_event_choice or week_summary_overlay.visible
	launch_now_button.visible = can and not busy
	launch_now_button.disabled = not can or busy
	if state.launched_to_market:
		launch_now_button.tooltip_text = "Уже в рынке."
	elif not can:
		launch_now_button.tooltip_text = state.get_launch_block_reason_text()
	elif state.launch_prerequisites_met_recommended():
		launch_now_button.tooltip_text = "Переход к этапу Fit (как задача «Вывести продукт на рынок»): −1200 ₽, сильный старт по активации и удержанию."
	else:
		launch_now_button.tooltip_text = (
			"Ранний переход к Fit: −1200 ₽, слабее старт в рынке, −мораль и +техдолг. "
			+ "Уверенный релиз — при CMO+CTO и CustDev/Problem/MVP≥6/6/7."
		)

func _on_launch_now_pressed() -> void:
	if state.game_over or state.launched_to_market or not state.can_launch_now():
		return
	if planning_locked or waiting_event_choice:
		return
	var msg: String = state.apply_immediate_market_launch()
	_set_status_note("%s\n\n%s" % [msg, status_note])
	session_history.append("[WEEK %d] [MARKET LAUNCH NOW]\n- %s" % [state.week, msg])
	_refresh_log_panel_text()
	_update_dashboard()
	prepare_week()

func _fmt_signed(v: float) -> String:
	var n := int(round(v))
	if n >= 0:
		return "+%d" % n
	return "%d" % n

func _metric_ru(m: String) -> String:
	match m:
		"activation":
			return "Активация"
		"retention":
			return "Удержание"
		"morale":
			return "Мораль"
		"tech_debt":
			return "Техдолг"
		"reputation":
			return "Репутация"
		_:
			return m

func _pulse_metrics_compact_bbcode() -> Array[String]:
	return _pulse_build_columns(true)

func _pulse_metrics_expanded_bbcode() -> Array[String]:
	return _pulse_build_columns(false)


## Кратко/подробно + раскрытие блоков по ролям: CFO — финансы и капитализация; HR — команда; CPO — продукт; CMO — каналы; CCO — выручка.
func _pulse_build_columns(compact: bool) -> Array[String]:
	_rebuild_selected_tasks_from_slots()
	var totals: Dictionary = state.preview_task_effect_totals(selected_tasks)
	var next_revenue: float = state.revenue + float(totals["revenue"])
	var next_burn: float = state.burn_rate + float(totals["burn_rate"])
	var projected_cash_delta: float = float(totals["cash"]) + (next_revenue - next_burn)
	var stage: Dictionary = state.get_stage_info()
	var acc: String = _bb_color_hex(UI_BORDER_ACCENT)
	var L: Array[String] = []
	var R: Array[String] = []

	if not compact:
		L.append("[color=%s][i]%s[/i][/color]\n\n" % [NEU_COLOR, "В скобках — дельта после фиксации спринта."])
		L.append(
			"[font_size=19][b]Неделя %d из %d[/b][/font_size]  [color=%s]· %s[/color]\n"
			% [state.week, GameState.TOTAL_WEEKS, acc, str(stage.get("name", ""))]
		)
		var goal: String = str(stage.get("goal", ""))
		if goal != "":
			L.append("  [color=%s]%s[/color]\n" % [NEU_COLOR, goal])
		L.append("\n")
	else:
		L.append("[font_size=19][b]Неделя %d/%d[/b][/font_size] [color=%s]· %s[/color]\n" % [state.week, GameState.TOTAL_WEEKS, acc, str(stage.get("name", ""))])

	L.append(_pulse_section_title("Деньги"))
	if state.has_cfo:
		L.append(_pulse_kv("Кэш", "%.0f ₽ %s" % [state.cash, _fmt_delta_bbcode(projected_cash_delta, "₽")]))
		L.append(_pulse_kv("Runway", "%.1f мес" % state.get_runway_months()))
		L.append(_pulse_kv("MRR", "%.0f ₽ %s" % [state.revenue, _fmt_delta_bbcode(float(totals["revenue"]), "₽/мес")]))
		L.append(_pulse_kv("Burn", "%.0f ₽ %s" % [state.burn_rate, _fmt_delta_bbcode(float(totals["burn_rate"]), "₽/мес")]))
		L.append("  [color=%s]%s[/color]\n" % [NEU_COLOR, state.get_budget_breakdown_text()])
	else:
		L.append(_pulse_kv("Кэш", "%.0f ₽ %s" % [state.cash, _fmt_delta_bbcode(projected_cash_delta, "₽")]))
		L.append(_pulse_kv("Runway", "%.1f мес" % state.get_runway_months()))
		L.append(
			_pulse_kv(
				"MRR / Burn",
				"%.0f / %.0f ₽ %s %s"
				% [
					state.revenue,
					state.burn_rate,
					_fmt_delta_bbcode(float(totals["revenue"]), "₽"),
					_fmt_delta_bbcode(float(totals["burn_rate"]), "₽"),
				]
			)
		)

	if state.has_cco and state.launched_to_market:
		L.append("\n")
		L.append(_pulse_section_title("Выручка (CCO)"))
		for line: String in state.get_revenue_detail_pulse_lines():
			L.append("  [color=%s]%s[/color]\n" % [NEU_COLOR, line])

	L.append("\n")
	L.append(_pulse_section_title("Продукт"))
	if state.has_cpo:
		L.append(_pulse_kv("Активация", "%.0f %s" % [state.activation, _fmt_delta_bbcode(float(totals.get("activation", 0.0)), "п.")]))
		L.append(_pulse_kv("Удержание", "%.0f %s" % [state.retention, _fmt_delta_bbcode(float(totals.get("retention", 0.0)), "п.")]))
		L.append(
			_pulse_kv(
				"PMF-база",
				"CD %.0f %s · проб. %.0f %s · MVP %.0f %s"
				% [
					state.custdev_points,
					_fmt_delta_bbcode(float(totals.get("custdev_points", 0.0)), "п."),
					state.problem_clarity,
					_fmt_delta_bbcode(float(totals.get("problem_clarity", 0.0)), "п."),
					state.mvp_progress,
					_fmt_delta_bbcode(float(totals.get("mvp_progress", 0.0)), "п."),
				]
			)
		)
		if state.has_playable_prototype:
			L.append(_pulse_kv("Прототип", "есть кликабельный для показа"))
		if state.has_technical_mvp:
			L.append(_pulse_kv("Тех. MVP", "есть backend/данные — аналитика даёт продуктовый сигнал"))
	elif not compact:
		L.append(
			_pulse_kv(
				"Активация / Удержание",
				"%.0f / %.0f %s %s"
				% [
					state.activation,
					state.retention,
					_fmt_delta_bbcode(float(totals.get("activation", 0.0)), "п."),
					_fmt_delta_bbcode(float(totals.get("retention", 0.0)), "п."),
				]
			)
		)
		L.append(_pulse_kv("PMF", "CD %.0f · Problem %.0f · MVP %.0f" % [state.custdev_points, state.problem_clarity, state.mvp_progress]))
	else:
		L.append(
			_pulse_kv(
				"Активация / Удержание",
				"%.0f / %.0f %s %s"
				% [
					state.activation,
					state.retention,
					_fmt_delta_bbcode(float(totals.get("activation", 0.0)), "п."),
					_fmt_delta_bbcode(float(totals.get("retention", 0.0)), "п."),
				]
			)
		)
		L.append(_pulse_kv("PMF", "CD %.0f · Problem %.0f · MVP %.0f" % [state.custdev_points, state.problem_clarity, state.mvp_progress]))

	if state.has_marketer:
		L.append("\n")
		L.append(_pulse_section_title("Каналы привлечения"))
		for line: String in state.get_acquisition_channels_pulse_lines():
			L.append("  [color=%s]%s[/color]\n" % [NEU_COLOR, line])

	if not compact:
		L.append(_pulse_kv("Рынок", "В рынке" if state.launched_to_market else "Не запущен"))
		L.append(_pulse_kv("Офис", "%s" % state.get_office_summary()))

	if not compact:
		R.append(_pulse_section_title("Цепочка в спринте"))
		R.append("  %s\n" % state.get_process_chain_title())
		R.append("  [color=%s]%s[/color]\n" % [NEU_COLOR, state.get_process_chain_legend_line()])
		R.append("  %s\n" % state.get_process_chain_bbcode(POS_COLOR, NEU_COLOR))
		R.append("\n")

	if state.has_cfo:
		R.append(_pulse_section_title("Капитализация"))
		R.append(
			_pulse_kv(
				"Мамкин",
				"%.0f%% %s · прочие %.0f%%"
				% [
					state.founder_share_percent,
					_fmt_delta_bbcode(float(totals.get("founder_share", 0.0)), "п.п."),
					state.get_nonfounder_share_percent(),
				]
			)
		)
	else:
		R.append(_pulse_section_title("Команда и риск"))
		R.append(_pulse_kv("Доля Мамкина", "%.0f%% %s" % [state.founder_share_percent, _fmt_delta_bbcode(float(totals.get("founder_share", 0.0)), "п.п.")]))

	if compact:
		R.append(_pulse_kv("Рынок", "В рынке" if state.launched_to_market else "Не запущен"))
		R.append(_pulse_kv("Офис", "%s" % state.get_office_summary()))

	R.append("\n")
	if state.has_hr:
		R.append(_pulse_section_title("Команда"))
		R.append(_pulse_kv("Роли", "%s" % state.get_team_summary()))
		var perks_lines: Array[String] = []
		for line: String in str(state.get_team_perks_summary()).split("\n"):
			var t: String = line.strip_edges()
			if t != "":
				perks_lines.append(t)
		if not perks_lines.is_empty():
			R.append("  [b]Перки[/b]  %s\n" % perks_lines[0])
			if perks_lines.size() > 1:
				R.append("  [color=%s]... и ещё %d[/color]\n" % [NEU_COLOR, perks_lines.size() - 1])
		R.append(_pulse_kv("Мораль", "%.0f %s" % [state.morale, _fmt_delta_bbcode(float(totals.get("morale", 0.0)), "п.")]))
		R.append(_pulse_kv("Техдолг", "%.0f %s" % [state.tech_debt, _fmt_delta_bbcode(float(totals.get("tech_debt", 0.0)), "п.")]))
		R.append(_pulse_kv("Репутация", "%.0f %s" % [state.reputation, _fmt_delta_bbcode(float(totals.get("reputation", 0.0)), "п.")]))
	elif not compact:
		R.append("  [color=%s]Найми HR — появится расклад ролей, перки и динамика морали/техдолга/репутации.[/color]\n")

	if compact:
		R.append("  [color=%s]Подробный разбор: режим «подробно», пульс по мере найма CFO/HR/CPO/CMO/CCO или лог.[/color]\n" % NEU_COLOR)

	return ["".join(L), "".join(R)]

func _fmt_delta_bbcode(v: float, suffix: String) -> String:
	var n := int(round(v))
	if n > 0:
		return "[color=%s](+%d %s)[/color]" % [POS_COLOR, n, suffix]
	if n < 0:
		return "[color=%s](%d %s)[/color]" % [NEG_COLOR, n, suffix]
	return "[color=%s](0 %s)[/color]" % [NEU_COLOR, suffix]

func _bb_color_hex(c: Color) -> String:
	return "#" + c.to_html(false)

func _pulse_section_title(title: String) -> String:
	return "[font_size=18][color=%s][b]%s[/b][/color][/font_size]\n" % [_bb_color_hex(UI_TEXT_ACCENT), title]

func _pulse_kv(k: String, v_bbcode: String) -> String:
	return "  • [b]%s[/b]  %s\n" % [k, v_bbcode]

func _build_risk_badges() -> String:
	var alerts: Array[String] = []
	if state.get_runway_months() < 3.0:
		alerts.append("Критично: runway меньше 3 мес")
	elif state.get_runway_months() < 5.0:
		alerts.append("Внимание: runway меньше 5 мес")
	if state.morale < 30.0:
		alerts.append("Внимание: низкая мораль команды")
	if state.tech_debt > 70.0:
		alerts.append("Внимание: высокий техдолг")
	if state.retention < 25.0:
		alerts.append("Внимание: слабое удержание")
	if state.founder_share_percent < 55.0 and state.founder_share_percent + 0.001 >= 50.0:
		alerts.append("Внимание: доля Мамкина < 55% — близко к потере контроля")
	if alerts.is_empty():
		return "Риски: в норме\nМожно не паниковать (пока)."
	return "Риски — на радаре:\n• " + "\n• ".join(alerts)

func _update_risk_visuals() -> void:
	var severity: String = "ok"
	if state.get_runway_months() < 3.0 or state.morale < 20.0:
		severity = "critical"
	elif state.get_runway_months() < 5.0 or state.tech_debt > 70.0 or state.retention < 25.0 or (state.founder_share_percent < 55.0 and state.founder_share_percent + 0.001 >= 50.0):
		severity = "warning"

	match severity:
		"critical":
			risk_badge_label.add_theme_color_override("font_color", Color(1.0, 0.5, 0.48))
		"warning":
			risk_badge_label.add_theme_color_override("font_color", Color(1.0, 0.84, 0.42))
		_:
			risk_badge_label.add_theme_color_override("font_color", Color(0.72, 0.96, 0.78))

func _color_for_event_type(event_type: String) -> Color:
	match event_type:
		"Инвестор":
			return Color(0.82, 0.86, 1.0)
		"Финансы":
			return Color(0.78, 0.92, 0.85)
		"Адвайзер", "Эксперт":
			return Color(0.84, 0.9, 1.0)
		"Команда":
			return Color(0.78, 1.0, 0.85)
		"Продукт":
			return Color(0.96, 0.92, 1.0)
		"Рынок":
			return Color(1.0, 0.9, 0.75)
		"Техника":
			return Color(0.82, 0.95, 1.0)
		"PR/Репутация":
			return Color(1.0, 0.82, 0.9)
		_:
			return Color(0.92, 0.92, 0.92)

func _glass(c: Color, a: float) -> Color:
	return Color(c.r, c.g, c.b, a)


func _pixel_panel(bg: Color, border: Color, margin: int = 10) -> StyleBoxFlat:
	var style := StyleBoxFlat.new()
	style.bg_color = bg
	style.border_color = border
	style.border_width_left = 3
	style.border_width_top = 3
	style.border_width_right = 3
	style.border_width_bottom = 3
	style.corner_radius_top_left = 0
	style.corner_radius_top_right = 0
	style.corner_radius_bottom_right = 0
	style.corner_radius_bottom_left = 0
	style.content_margin_left = margin
	style.content_margin_top = margin
	style.content_margin_right = margin
	style.content_margin_bottom = margin
	return style

func _apply_panel_styles() -> void:
	var b_main := _glass(UI_BORDER, 0.42)
	sprint_panel.add_theme_stylebox_override(
		"panel", _pixel_panel(_glass(UI_PANEL_SPRINT, UI_GLASS_MAIN), b_main, 12)
	)
	center_panel.add_theme_stylebox_override(
		"panel", _pixel_panel(_glass(UI_PANEL_CENTER, UI_GLASS_MAIN), b_main, 12)
	)
	event_panel.add_theme_stylebox_override(
		"panel",
		_pixel_panel(_glass(UI_PANEL_EVENT, UI_GLASS_EVENT), _glass(UI_BORDER_ACCENT.darkened(0.35), 0.5), 12)
	)
	top_bar.add_theme_stylebox_override(
		"panel",
		_pixel_panel(_glass(UI_PANEL_HEADER, UI_GLASS_HEADER), _glass(UI_BORDER_ACCENT.darkened(0.2), 0.55), 14)
	)

func _style_sprint_slots() -> void:
	for s in sprint_slots:
		s.add_theme_stylebox_override(
			"panel", _pixel_panel(_glass(UI_SLOT_EMPTY, UI_GLASS_SLOT), _glass(UI_SLOT_BORDER, 0.45), 6)
		)

func _style_button_outline(btn: Button, bg: Color, bg_h: Color, border: Color) -> void:
	var n := StyleBoxFlat.new()
	n.bg_color = bg
	n.border_color = border
	n.border_width_left = 3
	n.border_width_top = 3
	n.border_width_right = 3
	n.border_width_bottom = 3
	n.content_margin_left = 14
	n.content_margin_top = 10
	n.content_margin_right = 14
	n.content_margin_bottom = 10
	var h := n.duplicate() as StyleBoxFlat
	h.bg_color = bg_h
	var p := n.duplicate() as StyleBoxFlat
	p.bg_color = bg.darkened(0.12)
	btn.add_theme_stylebox_override("normal", n)
	btn.add_theme_stylebox_override("hover", h)
	btn.add_theme_stylebox_override("pressed", p)
	btn.add_theme_stylebox_override("focus", n)
	btn.add_theme_color_override("font_color", UI_TEXT)
	btn.add_theme_color_override("font_hover_color", UI_TEXT)
	btn.add_theme_color_override("font_pressed_color", UI_TEXT_MUTED)

func _style_button_cta(btn: Button) -> void:
	_style_button_outline(btn, UI_BTN_CTA_BG, UI_BTN_CTA_BG_H, UI_BTN_CTA_BORDER)

func _style_buttons() -> void:
	_style_button_outline(log_toggle_button, UI_BTN_PRIMARY_BG, UI_BTN_PRIMARY_BG_H, UI_BORDER)
	_style_button_outline(pulse_mode_toggle_button, UI_BTN_PRIMARY_BG, UI_BTN_PRIMARY_BG_H, UI_BORDER)
	_style_button_outline(choose_a, UI_BTN_PRIMARY_BG, UI_BTN_PRIMARY_BG_H, UI_BORDER)
	_style_button_outline(choose_b, UI_BTN_PRIMARY_BG, UI_BTN_PRIMARY_BG_H, UI_BORDER)
	_style_button_cta(next_week_button)
	_style_button_cta(launch_now_button)

func _style_risk_badge() -> void:
	var band := UI_PANEL_CENTER.lightened(0.07)
	risk_badge_label.add_theme_stylebox_override(
		"normal", _pixel_panel(_glass(band, UI_GLASS_RISK), _glass(UI_BORDER_ACCENT.darkened(0.5), 0.55), 10)
	)

func _style_event_speaker_chip() -> void:
	var bg := UI_PANEL_EVENT.lightened(0.05)
	event_speaker_chip.add_theme_stylebox_override("normal", _pixel_panel(bg, UI_BORDER_ACCENT.darkened(0.45), 8))
	event_speaker_chip.add_theme_color_override("font_color", UI_TEXT)

func _welcome_card_style(accent: Color, left_border: int = 0) -> StyleBoxFlat:
	var sb := StyleBoxFlat.new()
	sb.bg_color = _glass(UI_PANEL_CENTER.lightened(0.06), 0.82)
	sb.border_color = _glass(accent, 0.72)
	sb.border_width_left = left_border if left_border > 0 else 1
	sb.border_width_top = 1
	sb.border_width_right = 1
	sb.border_width_bottom = 1
	sb.corner_radius_top_left = 12
	sb.corner_radius_top_right = 12
	sb.corner_radius_bottom_right = 12
	sb.corner_radius_bottom_left = 12
	return sb


func _rounded_glass_panel(bg: Color, border: Color, margin: int = 14, radius: int = 18, alpha: float = 0.56, border_alpha: float = 0.6) -> StyleBoxFlat:
	var sb := StyleBoxFlat.new()
	sb.bg_color = _glass(bg, alpha)
	sb.border_color = _glass(border, border_alpha)
	sb.border_width_left = 1
	sb.border_width_top = 1
	sb.border_width_right = 1
	sb.border_width_bottom = 1
	sb.corner_radius_top_left = radius
	sb.corner_radius_top_right = radius
	sb.corner_radius_bottom_right = radius
	sb.corner_radius_bottom_left = radius
	sb.content_margin_left = margin
	sb.content_margin_top = margin
	sb.content_margin_right = margin
	sb.content_margin_bottom = margin
	return sb


func _style_stage_intro() -> void:
	stage_intro_overlay.mouse_filter = Control.MOUSE_FILTER_STOP
	_style_button_cta(stage_intro_start_button)
	_style_button_cta(welcome_next_button)
	welcome_stage_card_build.add_theme_stylebox_override("panel", _welcome_card_style(UI_BORDER_ACCENT.lightened(0.12)))
	welcome_stage_card_fit.add_theme_stylebox_override("panel", _welcome_card_style(Color(0.76, 0.52, 0.96, 1.0)))
	welcome_stage_card_scale.add_theme_stylebox_override("panel", _welcome_card_style(Color(1.0, 0.72, 0.26, 1.0)))
	welcome_alert_block.add_theme_stylebox_override("panel", _welcome_card_style(Color(1.0, 0.56, 0.26, 1.0), 5))
	stage_intro_badge.add_theme_color_override("font_color", UI_TEXT_ACCENT)
	stage_intro_goal.add_theme_color_override("font_color", UI_TEXT)
	stage_intro_body.add_theme_color_override("font_color", UI_TEXT_MUTED)

func _style_log_overlay() -> void:
	log_overlay.mouse_filter = Control.MOUSE_FILTER_STOP
	log_overlay_dialog.add_theme_stylebox_override(
		"panel",
		_rounded_glass_panel(UI_PANEL_CENTER.lightened(0.03), UI_BORDER_ACCENT.darkened(0.2), 16, 20, 0.56, 0.62)
	)
	log_overlay_title.add_theme_color_override("font_color", UI_TEXT_ACCENT)
	log_overlay_hint.add_theme_color_override("font_color", UI_TEXT_MUTED)
	log_overlay_body.add_theme_color_override("font_color", UI_TEXT)
	_style_button_cta(log_overlay_save_button)
	_style_button_outline(log_overlay_close_button, UI_BTN_PRIMARY_BG, UI_BTN_PRIMARY_BG_H, UI_BORDER)

func _style_typography() -> void:
	title_label.add_theme_color_override("font_color", UI_TEXT)
	subtitle_label.add_theme_color_override("font_color", UI_TEXT_MUTED)
	metrics_title_label.add_theme_color_override("font_color", UI_TEXT_ACCENT)
	tasks_label.add_theme_color_override("font_color", UI_TEXT)
	slots_hint_label.add_theme_color_override("font_color", UI_TEXT_MUTED)
	event_title_label.add_theme_color_override("font_color", UI_TEXT_ACCENT)
	# Цвет рисков задаётся в _update_risk_visuals()
	event_label.add_theme_color_override("font_color", UI_TEXT)
	log_label.add_theme_color_override("font_color", UI_TEXT_MUTED)
	metrics_left.add_theme_color_override("default_color", UI_TEXT)
	metrics_right.add_theme_color_override("default_color", UI_TEXT)
	header_avatar.modulate = Color(1.0, 1.0, 1.0, 1.0)

func _style_scrollbar_bar(bar: ScrollBar) -> void:
	var grab := StyleBoxFlat.new()
	grab.bg_color = UI_BORDER.darkened(0.15)
	grab.border_color = UI_BORDER
	grab.border_width_left = 1
	grab.border_width_top = 1
	grab.border_width_right = 1
	grab.border_width_bottom = 1
	var grab_h := grab.duplicate() as StyleBoxFlat
	grab_h.bg_color = UI_BORDER_ACCENT.darkened(0.5)
	bar.add_theme_stylebox_override("grabber", grab)
	bar.add_theme_stylebox_override("grabber_highlight", grab_h)
	bar.add_theme_stylebox_override("grabber_pressed", grab_h)

func _style_scrollbars() -> void:
	_style_scrollbar_bar(log_scroll.get_v_scroll_bar())
	_style_scrollbar_bar(achievements_scroll.get_v_scroll_bar())
	_style_scrollbar_bar(log_overlay_scroll.get_v_scroll_bar())
	_style_scrollbar_bar(welcome_body_scroll.get_v_scroll_bar())
	_style_scrollbar_bar(welcome_body_scroll.get_h_scroll_bar())
	var esc: ScrollContainer = $Margin/Root/MainColumns/SprintPanel/SprintBox/BacklogScroll
	_style_scrollbar_bar(esc.get_v_scroll_bar())
	var evs: ScrollContainer = $LegacyEventHost/EventPanel/EventBox/EventScroll
	_style_scrollbar_bar(evs.get_v_scroll_bar())
	_style_scrollbar_bar(week_summary_body_scroll.get_v_scroll_bar())

func _style_task_card(card: TaskCard) -> void:
	card.add_theme_stylebox_override(
		"panel",
		_pixel_panel(_glass(UI_CARD_BASE, UI_GLASS_CARD), _glass(UI_BORDER_ACCENT.darkened(0.75), 0.5), 8)
	)
